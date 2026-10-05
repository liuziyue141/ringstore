use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use crate::rpc::storage::{
    backend_storage_client::BackendStorageClient, Clock, Key, KeyValue, Pattern,
};
use crate::Result;
use tokio::{sync::Mutex, task::JoinSet, time::sleep};
use tonic::transport::{Channel, Endpoint};

use super::{
    operation::{self, Operation},
    ring::Ring,
};
use crate::rpc::control::{
    backend_control_client::BackendControlClient, BackendInfo, ClockFloor, Empty,
};

pub const REPLICAS: usize = 3;
const RETRY_DELAY: Duration = Duration::from_millis(25);

pub struct Backend {
    pub address: String,
    pub slot: u64,
    channel: Mutex<Option<Channel>>,
}

impl Backend {
    pub fn new(address: String, slot: u64) -> Self {
        Self {
            address,
            slot,
            channel: Mutex::new(None),
        }
    }

    async fn channel(&self) -> Result<Channel> {
        let mut cached = self.channel.lock().await;
        if let Some(channel) = &*cached {
            return Ok(channel.clone());
        }
        let channel = Endpoint::from_shared(self.address.clone())?
            .connect_timeout(Duration::from_millis(300))
            .timeout(Duration::from_secs(25))
            .tcp_nodelay(true)
            .connect()
            .await?;
        *cached = Some(channel.clone());
        Ok(channel)
    }

    async fn invalidate(&self) {
        *self.channel.lock().await = None;
    }

    pub async fn inspect(&self) -> Result<BackendInfo> {
        let mut client = BackendControlClient::new(self.channel().await?);
        match client.inspect(Empty {}).await {
            Ok(response) => Ok(response.into_inner()),
            Err(error) => {
                self.invalidate().await;
                Err(error.into())
            }
        }
    }

    pub async fn advance_clock(&self, value: u64) -> Result<()> {
        let mut client = BackendControlClient::new(self.channel().await?);
        match client.advance_clock(ClockFloor { value }).await {
            Ok(_) => Ok(()),
            Err(error) => {
                self.invalidate().await;
                Err(error.into())
            }
        }
    }

    async fn clock(&self, at_least: u64) -> Result<u64> {
        let mut client = BackendStorageClient::new(self.channel().await?);
        match client
            .clock(Clock {
                timestamp: at_least,
            })
            .await
        {
            Ok(response) => Ok(response.into_inner().timestamp),
            Err(error) => {
                self.invalidate().await;
                Err(error.into())
            }
        }
    }

    pub async fn read_raw(&self, key: &str) -> Result<Vec<String>> {
        let mut client = BackendStorageClient::new(self.channel().await?);
        match client
            .list_get(Key {
                key: key.to_owned(),
            })
            .await
        {
            Ok(response) => Ok(response.into_inner().list),
            Err(error) => {
                self.invalidate().await;
                Err(error.into())
            }
        }
    }

    pub async fn keys(&self, prefix: &str) -> Result<Vec<String>> {
        let mut client = BackendStorageClient::new(self.channel().await?);
        match client
            .list_keys(Pattern {
                prefix: prefix.to_owned(),
                suffix: String::new(),
            })
            .await
        {
            Ok(response) => Ok(response.into_inner().list),
            Err(error) => {
                self.invalidate().await;
                Err(error.into())
            }
        }
    }

    /// A partial copy is safe to repeat: identity, not timestamp, defines the diff.
    pub async fn copy_missing(&self, key: &str, operations: &[Operation]) -> Result<()> {
        let existing = operation::decode(self.read_raw(key).await?)?;
        let missing = operation::difference(operations, &existing);
        let mut client = BackendStorageClient::new(self.channel().await?);
        for operation in missing {
            let result = client
                .list_append(KeyValue {
                    key: key.to_owned(),
                    value: serde_json::to_string(&operation)?,
                })
                .await;
            if let Err(error) = result {
                self.invalidate().await;
                return Err(error.into());
            }
        }
        Ok(())
    }
}

pub struct Cluster {
    pub ring: Ring,
}

impl Cluster {
    pub fn new(addresses: Vec<String>) -> Result<Self> {
        Ok(Self {
            ring: Ring::new(addresses)?,
        })
    }

    pub async fn live_nodes(&self) -> Result<Vec<(Arc<Backend>, BackendInfo)>> {
        loop {
            let mut tasks = JoinSet::new();
            for node in &self.ring.nodes {
                let node = Arc::clone(node);
                tasks.spawn(async move {
                    let result = node.inspect().await;
                    (node, result)
                });
            }
            let mut live = Vec::new();
            while let Some(result) = tasks.join_next().await {
                let (node, info) = result?;
                if let Ok(info) = info {
                    live.push((node, info));
                }
            }
            if !live.is_empty() {
                return Ok(live);
            }
            sleep(RETRY_DELAY).await;
        }
    }

    /// Include retained copies: a newly reachable successor may still be empty.
    /// Extra read RPCs keep handoff independent of frontend/keeper coordination.
    pub async fn read_log(&self, key: &str) -> Result<Vec<Operation>> {
        loop {
            let mut tasks = JoinSet::new();
            for node in &self.ring.nodes {
                let node = Arc::clone(node);
                let key = key.to_owned();
                tasks.spawn(async move { node.read_raw(&key).await });
            }
            let mut logs = Vec::new();
            while let Some(result) = tasks.join_next().await {
                if let Ok(raw) = result? {
                    logs.push(operation::decode(raw)?);
                }
            }
            if !logs.is_empty() {
                return operation::merge(logs);
            }
            sleep(RETRY_DELAY).await;
        }
    }

    pub async fn keys(&self, prefix: &str) -> Result<BTreeSet<String>> {
        loop {
            let mut tasks = JoinSet::new();
            for node in &self.ring.nodes {
                let node = Arc::clone(node);
                let prefix = prefix.to_owned();
                tasks.spawn(async move { node.keys(&prefix).await });
            }
            let mut successes = 0;
            let mut keys = BTreeSet::new();
            while let Some(result) = tasks.join_next().await {
                if let Ok(found) = result? {
                    successes += 1;
                    keys.extend(found);
                }
            }
            if successes > 0 {
                return Ok(keys);
            }
            sleep(RETRY_DELAY).await;
        }
    }

    /// Check sequentially, writing complete copies to three distinct backends.
    /// Recheck live incarnations before finishing on a potentially stale set.
    pub async fn replicate(&self, bin: &str, key: &str, operations: &[Operation]) -> Result<()> {
        if operations.is_empty() {
            return Ok(());
        }
        loop {
            let mut copied = HashMap::new();
            for node in self.ring.clockwise(bin) {
                let Ok(before) = node.inspect().await else {
                    continue;
                };
                if node.copy_missing(key, operations).await.is_ok() {
                    copied.insert(node.address.clone(), before.incarnation);
                    if copied.len() == REPLICAS {
                        break;
                    }
                }
            }
            let live = self.live_nodes().await?;
            let desired: Vec<_> = self
                .ring
                .clockwise(bin)
                .filter_map(|node| live.iter().find(|(n, _)| n.address == node.address))
                .take(REPLICAS)
                .collect();
            if desired
                .iter()
                .all(|(node, info)| copied.get(&node.address) == Some(&info.incarnation))
            {
                return Ok(());
            }
            sleep(RETRY_DELAY).await;
        }
    }

    pub async fn publish_clock(&self, value: u64) -> Result<usize> {
        let mut tasks = JoinSet::new();
        for node in &self.ring.nodes {
            let node = Arc::clone(node);
            tasks.spawn(async move { node.advance_clock(value).await });
        }
        let mut successes = 0;
        while let Some(result) = tasks.join_next().await {
            if result?.is_ok() {
                successes += 1;
            }
        }
        Ok(successes)
    }

    /// Atomic backend counters occupy disjoint numeric slots. Publish the floor
    /// before confirming that the allocating process has not restarted.
    pub async fn clock(&self, at_least: u64) -> Result<u64> {
        loop {
            let live = self.live_nodes().await?;
            let floor = live
                .iter()
                .map(|(_, info)| info.clock_floor)
                .max()
                .unwrap_or(0);
            let bound = at_least.max(floor.saturating_add(1));
            let width = self.ring.nodes.len() as u128;
            for node in self.ring.clockwise("__ringstore_clock") {
                let Some((_, info)) = live.iter().find(|(n, _)| n.address == node.address) else {
                    continue;
                };
                let tick = u128::from(bound)
                    .saturating_sub(u128::from(node.slot))
                    .div_ceil(width);
                let Ok(tick) = node.clock(tick as u64).await else {
                    continue;
                };
                let value = (u128::from(tick) * width + u128::from(node.slot))
                    .min(u128::from(u64::MAX)) as u64;
                if self.publish_clock(value).await? == 0 {
                    break;
                }
                if let Ok(after) = node.inspect().await {
                    if after.incarnation == info.incarnation {
                        return Ok(value);
                    }
                }
                break;
            }
            sleep(RETRY_DELAY).await;
        }
    }
}
