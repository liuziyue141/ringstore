use std::{net::SocketAddr, sync::Arc};

use crate::{
    config::BackendConfig, rpc::storage::backend_storage_server::BackendStorageServer,
    storage::Storage, Result,
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use super::{
    client::StorageClient,
    server::{BackendMetadata, StorageServer},
};
use crate::rpc::control::backend_control_server::BackendControlServer;

/// Bind before signalling readiness; both services belong to this server future.
pub async fn serve_back(mut config: BackendConfig) -> Result<()> {
    let listener = match async {
        let address: SocketAddr = config.address.parse()?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(TcpListener::bind(address).await?)
    }
    .await
    {
        Ok(listener) => listener,
        Err(error) => {
            if let Some(ready) = config.ready {
                let _ = ready.send(false);
            }
            return Err(error);
        }
    };
    let server = Server::builder()
        .add_service(BackendStorageServer::new(StorageServer {
            storage: Arc::from(config.storage),
        }))
        .add_service(BackendControlServer::new(BackendMetadata::default()));
    if let Some(ready) = config.ready {
        let _ = ready.send(true);
    }
    server
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            match config.shutdown.as_mut() {
                Some(shutdown) => {
                    shutdown.recv().await;
                }
                None => std::future::pending::<()>().await,
            }
        })
        .await?;
    Ok(())
}

pub async fn new_client(addr: &str) -> Result<Box<dyn Storage>> {
    Ok(Box::new(StorageClient {
        addr: addr.to_owned(),
    }))
}
