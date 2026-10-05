use std::sync::mpsc::Sender;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Receiver;

use crate::{storage::Storage, Result};

/// Identical backend membership must be supplied to every client and keeper.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub backends: Vec<String>,
    #[serde(default)]
    pub keepers: Vec<String>,
}

impl Config {
    pub fn read(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Ok(serde_json::from_slice(&std::fs::read(path)?)?)
    }
}

/// Readiness is signalled after binding; dropping the server future stops it.
pub struct BackendConfig {
    pub address: String,
    pub storage: Box<dyn Storage>,
    pub ready: Option<Sender<bool>>,
    pub shutdown: Option<Receiver<()>>,
}

/// Keeper state is disposable. A restart uses a fresh incarnation identifier.
pub struct KeeperConfig {
    pub backends: Vec<String>,
    pub keepers: Vec<String>,
    pub index: usize,
    pub incarnation: u128,
    pub ready: Option<Sender<bool>>,
    pub shutdown: Option<Receiver<()>>,
}
