use std::collections::BTreeSet;
use std::sync::Arc;

use crate::{
    storage::{KeyList, KeyString, KeyValue, List, Pattern, Storage},
    Result,
};
use async_trait::async_trait;

use super::{
    key_codec::{self, Kind},
    operation::{self, Action, Operation},
    replication::Cluster,
};

pub struct BinClient {
    pub name: String,
    pub(crate) cluster: Arc<Cluster>,
}

impl BinClient {
    async fn write(&self, key: &str, kind: Kind, action: Action) -> Result<()> {
        // One identity is reused for every delivery and retry.
        let encoded = key_codec::encode(&self.name, kind, key);
        let clock = self.cluster.clock(0).await?;
        let timestamp = if clock == u64::MAX {
            // The public clock may repeat MAX, but sequential storage mutations
            // must still advance. Widen the per-key logical order past that bound.
            let previous = self
                .cluster
                .read_log(&encoded)
                .await?
                .last()
                .map(|operation| operation.timestamp)
                .unwrap_or(0);
            u128::from(clock).max(previous + 1)
        } else {
            u128::from(clock)
        };
        let operation = Operation::new(timestamp, action);
        self.cluster
            .replicate(&self.name, &encoded, &[operation])
            .await
    }

    async fn enumerate(&self, kind: Kind, pattern: &Pattern) -> Result<List> {
        let mut result = BTreeSet::new();
        let prefix = key_codec::prefix(&self.name, kind);
        for encoded in self.cluster.keys(&prefix).await? {
            let Some((_, _, key)) = key_codec::decode(&encoded) else {
                continue;
            };
            if !pattern.matches(&key) {
                continue;
            }
            let nonempty = match kind {
                Kind::Scalar => self.get(&key).await?.is_some(),
                Kind::List => !self.list_get(&key).await?.0.is_empty(),
            };
            if nonempty {
                result.insert(key);
            }
        }
        Ok(List(result.into_iter().collect()))
    }
}

#[async_trait]
impl KeyString for BinClient {
    async fn get(&self, key: &str) -> Result<Option<String>> {
        let encoded = key_codec::encode(&self.name, Kind::Scalar, key);
        let log = self.cluster.read_log(&encoded).await?;
        let value = operation::scalar(&log)?;
        // A read must not expose a value that vanishes with its only holder.
        // This also repairs deletion tombstones rather than resurrecting old sets.
        if let Some(latest) = log.last() {
            self.cluster
                .replicate(&self.name, &encoded, std::slice::from_ref(latest))
                .await?;
        }
        Ok(value)
    }

    async fn set(&self, kv: &KeyValue) -> Result<bool> {
        self.write(&kv.key, Kind::Scalar, Action::Set(kv.value.clone()))
            .await?;
        Ok(true)
    }

    async fn keys(&self, pattern: &Pattern) -> Result<List> {
        self.enumerate(Kind::Scalar, pattern).await
    }
}

#[async_trait]
impl KeyList for BinClient {
    async fn list_get(&self, key: &str) -> Result<List> {
        let log = self
            .cluster
            .read_log(&key_codec::encode(&self.name, Kind::List, key))
            .await?;
        Ok(List(
            operation::list(&log)?
                .into_iter()
                .map(|(_, value)| value)
                .collect(),
        ))
    }

    async fn list_append(&self, kv: &KeyValue) -> Result<bool> {
        self.write(&kv.key, Kind::List, Action::Append(kv.value.clone()))
            .await?;
        Ok(true)
    }

    async fn list_remove(&self, kv: &KeyValue) -> Result<u32> {
        let encoded = key_codec::encode(&self.name, Kind::List, &kv.key);
        let log = self.cluster.read_log(&encoded).await?;
        let ids: Vec<_> = operation::list(&log)?
            .into_iter()
            .filter(|(_, value)| value == &kv.value)
            .map(|(id, _)| id)
            .collect();
        let count = u32::try_from(ids.len())?;
        if !ids.is_empty() {
            self.write(&kv.key, Kind::List, Action::Remove(ids)).await?;
        }
        Ok(count)
    }

    async fn list_keys(&self, pattern: &Pattern) -> Result<List> {
        self.enumerate(Kind::List, pattern).await
    }
}

#[async_trait]
impl Storage for BinClient {
    async fn clock(&self, at_least: u64) -> Result<u64> {
        self.cluster.clock(at_least).await
    }
}
