use std::{net::SocketAddr, sync::mpsc::Sender, time::Duration};

use crate::{config::KeeperConfig, storage::BinStorage, Result};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server as RpcServer;

use super::{client::Client, keeper::RepairKeeper};
use crate::rpc::control::keeper_server::KeeperServer;

pub async fn new_bin_client(backends: Vec<String>) -> Result<Box<dyn BinStorage>> {
    Ok(Box::new(Client::new(backends)?))
}

/// The RPC server and reconciliation future share one lifetime. Dropping the
/// worker also aborts its owned JoinSets, including on abrupt task cancellation.
pub async fn serve_keeper(mut config: KeeperConfig) -> Result<()> {
    let initialized = async {
        let keeper = RepairKeeper::new(
            config.backends.clone(),
            config.keepers.clone(),
            config.index,
            config.incarnation,
        )?;
        let address: SocketAddr = config.keepers[config.index].parse()?;
        let listener = TcpListener::bind(address).await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((keeper, listener))
    }
    .await;
    let (keeper, listener) = match initialized {
        Ok(value) => value,
        Err(error) => {
            if let Some(ready) = config.ready {
                let _ = ready.send(false);
            }
            return Err(error);
        }
    };
    let server = RpcServer::builder()
        .add_service(KeeperServer::new(keeper.clone()))
        .serve_with_incoming(TcpListenerStream::new(listener));
    let worker = reconcile_forever(keeper, config.ready.take());
    let shutdown = async {
        match config.shutdown.as_mut() {
            Some(shutdown) => {
                shutdown.recv().await;
            }
            None => std::future::pending::<()>().await,
        }
    };
    tokio::select! {
        result = server => { result?; }
        result = worker => { result?; }
        _ = shutdown => {}
    }
    Ok(())
}

async fn reconcile_forever(keeper: RepairKeeper, ready: Option<Sender<bool>>) -> Result<()> {
    keeper.reconcile().await?;
    if let Some(ready) = ready {
        let _ = ready.send(true);
    }
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if keeper.should_reconcile().await {
            if let Err(error) = keeper.reconcile().await {
                log::debug!("keeper reconciliation will retry: {error}");
            }
        }
    }
}
