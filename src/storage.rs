//! Public storage interfaces and an ephemeral in-memory backend.
use async_trait::async_trait;
use std::{collections::HashMap, sync::RwLock};

use crate::Result;

#[derive(Debug, Clone)]

/// A type comprising key-value pair
pub struct KeyValue {
    /// the key
    pub key: String,
    /// the value
    pub value: String,
}

impl KeyValue {
    /// convenience method for constructing a [KeyValue] from two `&str`s
    pub fn new(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: value.to_string(),
        }
    }
}

#[derive(Debug, Clone, Default)]
/// A type which represents a pattern that can be used to match on a String.
pub struct Pattern {
    /// exact-match string prefix
    pub prefix: String,
    /// exact-match string suffix
    pub suffix: String,
}

impl Pattern {
    /// this function returns true the provided string matches the prefix and
    /// suffix of the given pattern
    pub fn matches(&self, k: &str) -> bool {
        k.starts_with(&self.prefix) && k.ends_with(&self.suffix)
    }
}

#[derive(Debug, Clone)]
/// A wrapper type around a [Vec<String>]
pub struct List(pub Vec<String>);

#[async_trait]
/// Key-value pair interfaces
/// Default value for all keys is empty string
pub trait KeyString {
    /// Gets a value. If no value set, return [None]
    async fn get(&self, key: &str) -> Result<Option<String>>;

    /// Set kv.key to kv.value. return true when no error.
    async fn set(&self, kv: &KeyValue) -> Result<bool>;

    /// List all the keys of non-empty pairs where the key matches
    /// the given pattern.
    async fn keys(&self, p: &Pattern) -> Result<List>;
}

#[async_trait]
/// Key-list interfaces
pub trait KeyList {
    /// Get the list. Empty if not set.
    async fn list_get(&self, key: &str) -> Result<List>;

    /// Append a string to the list. return true when no error.
    async fn list_append(&self, kv: &KeyValue) -> Result<bool>;

    /// Removes all elements that are equal to `kv.value` in list `kv.key`
    /// returns the number of elements removed.
    async fn list_remove(&self, kv: &KeyValue) -> Result<u32>;

    /// List all the keys of non-empty lists, where the key matches
    /// the given pattern.
    async fn list_keys(&self, p: &Pattern) -> Result<List>;
}

#[async_trait]
/// A trait representing a storage interface
/// The trait bounds for [KeyString] and [KeyList] respectively represent
/// the functions requires for the single key-value and key-list parts of the
/// storage interface.
pub trait Storage: KeyString + KeyList + Send + Sync {
    /// Returns an auto-incrementing clock. The returned value of each call will
    /// be unique, no smaller than `at_least`, and strictly larger than the
    /// value returned last time, unless it was [u64::MAX]
    async fn clock(&self, at_least: u64) -> Result<u64>;
}

/// This is a toy implementation of a backend storage service.
/// The trait definition requires this to be safe to utilize across threads
/// because mutating methods (e.g. [KeyString::set] take `&self` instead of
/// `&mut self`)
#[derive(Debug, Default)]
pub struct MemStorage {
    kvs: RwLock<HashMap<String, String>>,
    kv_list: RwLock<HashMap<String, List>>,
    clock: RwLock<u64>,
}

impl MemStorage {
    /// Creates a new instance of [MemStorage]
    pub fn new() -> MemStorage {
        MemStorage::default()
    }
}

#[async_trait]
impl KeyString for MemStorage {
    async fn get(&self, key: &str) -> Result<Option<String>> {
        match self.kvs.read().map_err(|e| e.to_string())?.get(key) {
            Some(v) => Ok(Some(v.to_string())),
            None => Ok(None),
        }
    }

    async fn set(&self, kv: &KeyValue) -> Result<bool> {
        let mut entry = self.kvs.write().map_err(|e| e.to_string())?;
        if kv.value.is_empty() {
            entry.remove(&kv.key);
        } else {
            entry.insert(kv.key.clone(), kv.value.clone());
        }
        Ok(true)
    }

    async fn keys(&self, p: &Pattern) -> Result<List> {
        let result = self
            .kvs
            .read()
            .map_err(|e| e.to_string())?
            .iter()
            .filter(|(k, _)| p.matches(k))
            .map(|(k, _)| k.to_string())
            .collect::<Vec<String>>();
        Ok(List(result))
    }
}

#[async_trait]
impl KeyList for MemStorage {
    async fn list_get(&self, key: &str) -> Result<List> {
        match self.kv_list.read().map_err(|e| e.to_string())?.get(key) {
            Some(l) => Ok(l.clone()),
            None => Ok(List(vec![])),
        }
    }

    async fn list_append(&self, kv: &KeyValue) -> Result<bool> {
        let mut kvl = self.kv_list.write().map_err(|e| e.to_string())?;
        match kvl.get_mut(&kv.key) {
            Some(list) => {
                list.0.push(kv.value.clone());
                Ok(true)
            }
            None => {
                let list = vec![kv.value.clone()];
                kvl.insert(kv.key.clone(), List(list));
                Ok(true)
            }
        }
    }

    async fn list_remove(&self, kv: &KeyValue) -> Result<u32> {
        let mut removed = 0;
        let mut kvl = self.kv_list.write().map_err(|e| e.to_string())?;
        kvl.entry(kv.key.clone()).and_modify(|list| {
            let begin_size = list.0.len();
            *list = List(
                list.0
                    .iter()
                    .filter(|val| **val != kv.value)
                    .map(String::from)
                    .collect::<Vec<String>>(),
            );
            let end_size = list.0.len();
            removed = begin_size - end_size;
        });
        if let Some(x) = kvl.get(&kv.key) {
            if x.0.is_empty() {
                kvl.remove(&kv.key);
            }
        };

        Ok(removed as u32)
    }

    async fn list_keys(&self, p: &Pattern) -> Result<List> {
        let mut result = vec![];
        self.kv_list
            .read()
            .map_err(|e| e.to_string())?
            .iter()
            .filter(|(k, _)| p.matches(k))
            .for_each(|(v, _)| result.push((*v).clone()));
        result.sort();
        Ok(List(result))
    }
}

#[async_trait]
impl Storage for MemStorage {
    async fn clock(&self, at_least: u64) -> Result<u64> {
        let mut clk = self.clock.write().map_err(|e| e.to_string())?;
        if *clk < at_least {
            *clk = at_least
        }

        let ret = *clk;

        *clk = clk.saturating_add(1);
        Ok(ret)
    }
}

#[async_trait]
/// Bin Storage interface
pub trait BinStorage: Send + Sync {
    /// Fetch a [Storage] bin based on the given bin name.
    async fn bin(&self, name: &str) -> Result<Box<dyn Storage>>;
}
