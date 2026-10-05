use std::sync::Arc;

use crate::{
    storage::{BinStorage, Storage},
    Result,
};
use async_trait::async_trait;

use super::{bin::BinClient, replication::Cluster};

#[derive(Clone)]
pub struct Client {
    cluster: Arc<Cluster>,
}

impl Client {
    pub fn new(backends: Vec<String>) -> Result<Self> {
        Ok(Self {
            cluster: Arc::new(Cluster::new(backends)?),
        })
    }
}

#[async_trait]
impl BinStorage for Client {
    async fn bin(&self, name: &str) -> Result<Box<dyn Storage>> {
        Ok(Box::new(BinClient {
            name: name.to_owned(),
            cluster: Arc::clone(&self.cluster),
        }))
    }
}
