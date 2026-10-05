use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use crate::Result;
use tokio::task::JoinSet;
use tonic::{transport::Endpoint, Request, Response, Status};

use super::{
    key_codec::{self, LOG_PREFIX},
    operation::{self, Operation},
    replication::{Backend, Cluster, REPLICAS},
    ring::normalize_address,
};
use crate::rpc::control::{keeper_client::KeeperClient, keeper_server::Keeper, Empty, KeeperInfo};

struct DestinationCopies {
    node: Arc<Backend>,
    logs: Vec<(String, Vec<Operation>)>,
}

#[derive(Clone)]
pub struct RepairKeeper {
    cluster: Arc<Cluster>,
    addresses: Vec<String>,
    index: usize,
    id: u128,
}

impl RepairKeeper {
    pub fn new(
        backends: Vec<String>,
        addresses: Vec<String>,
        index: usize,
        id: u128,
    ) -> Result<Self> {
        if index >= addresses.len() {
            return Err("keeper index is out of range".into());
        }
        Ok(Self {
            cluster: Arc::new(Cluster::new(backends)?),
            addresses: addresses
                .iter()
                .map(|a| normalize_address(a))
                .collect::<Result<_>>()?,
            index,
            id,
        })
    }

    /// Election only reduces duplicate work. Overlapping repair is safe, so a
    /// keeper joining, dying, or changing incarnation cannot strand a migration.
    pub async fn should_reconcile(&self) -> bool {
        for address in &self.addresses[..self.index] {
            let result = async {
                let channel = Endpoint::from_shared(address.clone())?
                    .connect_timeout(Duration::from_millis(300))
                    .timeout(Duration::from_secs(1))
                    .connect()
                    .await?;
                KeeperClient::new(channel).heart_beat(Empty {}).await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            }
            .await;
            if result.is_ok() {
                return false;
            }
        }
        true
    }

    /// Reconstruct the repair from retained logs on every pass. No cursor,
    /// migration phase, or random target exists only in this process's memory.
    pub async fn reconcile(&self) -> Result<()> {
        let live = self.cluster.live_nodes().await?;
        let mut floor = live
            .iter()
            .map(|(_, info)| info.clock_floor)
            .max()
            .unwrap_or(0);
        let mut tasks = JoinSet::new();
        for (node, _) in &live {
            let node = Arc::clone(node);
            tasks.spawn(async move {
                let mut logs = Vec::new();
                for key in node.keys(LOG_PREFIX).await? {
                    let entries = node.read_raw(&key).await?;
                    logs.push((key, operation::decode(entries)?));
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(logs)
            });
        }
        let mut sources: BTreeMap<String, Vec<Vec<Operation>>> = BTreeMap::new();
        while let Some(result) = tasks.join_next().await {
            // A backend can fail midway through its scan. Other retained holders
            // supply the history; the next pass covers any newly discovered keys.
            if let Ok(logs) = result? {
                for (key, operations) in logs {
                    for operation in &operations {
                        floor = floor.max(operation.timestamp.min(u128::from(u64::MAX)) as u64);
                    }
                    sources.entry(key).or_default().push(operations);
                }
            }
        }
        self.cluster.publish_clock(floor).await?;

        // Group work by destination so repairs run across backends in parallel.
        let mut copies: HashMap<String, DestinationCopies> = HashMap::new();
        for (key, logs) in sources {
            let Some((bin, _, _)) = key_codec::decode(&key) else {
                continue;
            };
            let operations = operation::merge(logs)?;
            for node in self
                .cluster
                .ring
                .clockwise(&bin)
                .filter(|node| live.iter().any(|(n, _)| n.address == node.address))
                .take(REPLICAS)
            {
                copies
                    .entry(node.address.clone())
                    .or_insert_with(|| DestinationCopies {
                        node: Arc::clone(node),
                        logs: Vec::new(),
                    })
                    .logs
                    .push((key.clone(), operations.clone()));
            }
        }
        let mut tasks = JoinSet::new();
        for (_, destination) in copies {
            tasks.spawn(async move {
                for (key, operations) in destination.logs {
                    destination.node.copy_missing(&key, &operations).await?;
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            });
        }
        while let Some(result) = tasks.join_next().await {
            if let Err(error) = result? {
                log::debug!("repair interrupted; will retry: {error}");
            }
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl Keeper for RepairKeeper {
    async fn heart_beat(&self, _: Request<Empty>) -> Result<Response<KeeperInfo>, Status> {
        Ok(Response::new(KeeperInfo {
            addr: self.addresses[self.index].clone(),
            id_1: (self.id >> 64) as u64,
            id_2: self.id as u64,
            this: self.index as u32,
        }))
    }
}
