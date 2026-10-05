use async_trait::async_trait;

use crate::rpc::storage as protocol;
use crate::rpc::storage::backend_storage_client::BackendStorageClient;
use crate::storage::{KeyList, KeyString, KeyValue, List, Pattern, Storage};
use crate::Result;
use tonic::Code;
pub struct StorageClient {
    pub addr: String,
}

#[async_trait]
impl KeyString for StorageClient {
    /// Gets a value. If no value set, return [None]
    async fn get(&self, key: &str) -> Result<Option<String>> {
        let mut client = BackendStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .get(protocol::Key {
                key: key.to_string(),
            })
            .await;
        match r {
            Ok(result) => return Ok(Some(result.into_inner().value)),
            Err(e) => {
                if e.code() == Code::NotFound {
                    return Ok(None);
                } else {
                    return Err(Box::new(e));
                };
            }
        }
    }

    /// Set kv.key to kv.value. return true when no error.
    async fn set(&self, kv: &KeyValue) -> Result<bool> {
        let mut client = BackendStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .set(protocol::KeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await?;
        Ok(r.into_inner().value)
    }

    /// List all the keys of non-empty pairs where the key matches
    /// the given pattern.
    async fn keys(&self, p: &Pattern) -> Result<List> {
        let mut client = BackendStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .keys(protocol::Pattern {
                prefix: p.prefix.clone(),
                suffix: p.suffix.clone(),
            })
            .await?;
        Ok(crate::storage::List(r.into_inner().list))
    }
}

#[async_trait]
impl KeyList for StorageClient {
    async fn list_get(&self, key: &str) -> Result<List> {
        let mut client = BackendStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .list_get(protocol::Key {
                key: key.to_string(),
            })
            .await?;
        Ok(crate::storage::List(r.into_inner().list))
    }

    /// Append a string to the list. return true when no error.
    async fn list_append(&self, kv: &KeyValue) -> Result<bool> {
        let mut client = BackendStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .list_append(protocol::KeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await?;
        Ok(r.into_inner().value)
    }

    /// Removes all elements that are equal to `kv.value` in list `kv.key`
    /// returns the number of elements removed.
    async fn list_remove(&self, kv: &KeyValue) -> Result<u32> {
        let mut client = BackendStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .list_remove(protocol::KeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await?;
        Ok(r.into_inner().removed)
    }

    /// List all the keys of non-empty lists, where the key matches
    /// the given pattern.
    async fn list_keys(&self, p: &Pattern) -> Result<List> {
        let mut client = BackendStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .list_keys(protocol::Pattern {
                prefix: p.prefix.clone(),
                suffix: p.suffix.clone(),
            })
            .await?;
        Ok(crate::storage::List(r.into_inner().list))
    }
}

#[async_trait]
impl Storage for StorageClient {
    async fn clock(&self, at_least: u64) -> Result<u64> {
        let mut client = BackendStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .clock(protocol::Clock {
                timestamp: at_least,
            })
            .await?;
        Ok(r.into_inner().timestamp)
    }
}
