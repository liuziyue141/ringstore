use crate::rpc::control::{backend_control_server::BackendControl, BackendInfo, ClockFloor, Empty};
use crate::rpc::storage::backend_storage_server::BackendStorage;
use crate::rpc::storage::{
    Bool, Clock, Key, KeyValue, ListRemoveResponse, Pattern, StringList, Value,
};
use crate::storage;
use crate::storage::Storage;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tonic::Response;
use tonic::Status;

/// Process metadata stays outside the user-visible key spaces.
pub struct BackendMetadata {
    incarnation: String,
    clock_floor: AtomicU64,
}

impl Default for BackendMetadata {
    fn default() -> Self {
        Self {
            incarnation: format!("{:032x}", rand::random::<u128>()),
            clock_floor: AtomicU64::new(0),
        }
    }
}

#[tonic::async_trait]
impl BackendControl for BackendMetadata {
    async fn inspect(&self, _: tonic::Request<Empty>) -> Result<Response<BackendInfo>, Status> {
        Ok(Response::new(BackendInfo {
            incarnation: self.incarnation.clone(),
            clock_floor: self.clock_floor.load(Ordering::SeqCst),
        }))
    }

    async fn advance_clock(
        &self,
        request: tonic::Request<ClockFloor>,
    ) -> Result<Response<Empty>, Status> {
        self.clock_floor
            .fetch_max(request.into_inner().value, Ordering::SeqCst);
        Ok(Response::new(Empty {}))
    }
}

pub struct StorageServer {
    pub storage: Arc<dyn Storage>,
}

#[async_trait::async_trait]
impl BackendStorage for StorageServer {
    async fn get(
        &self,
        request: tonic::Request<Key>,
    ) -> Result<tonic::Response<Value>, tonic::Status> {
        let storage = &self.storage;
        let trib_result = storage.get(&request.into_inner().key).await;
        match trib_result {
            Ok(Some(value)) => Ok(Response::new(Value { value })),
            Ok(None) => Err(Status::not_found("key not found")),
            Err(e) => Err(Status::aborted(format!("{}", e))),
        }
    }
    async fn set(
        &self,
        request: tonic::Request<KeyValue>,
    ) -> Result<tonic::Response<Bool>, tonic::Status> {
        let storage = &self.storage;
        let key_value = request.into_inner();
        let trib_result = storage
            .set(&storage::KeyValue {
                key: key_value.key,
                value: key_value.value,
            })
            .await;
        match trib_result {
            Ok(value) => Ok(Response::new(Bool { value })),
            Err(e) => Err(Status::aborted(format!("{}", e))),
        }
    }
    async fn keys(
        &self,
        request: tonic::Request<Pattern>,
    ) -> Result<tonic::Response<StringList>, tonic::Status> {
        let storage = &self.storage;
        let pattern = request.into_inner();
        let trib_result = storage
            .keys(&storage::Pattern {
                prefix: pattern.prefix,
                suffix: pattern.suffix,
            })
            .await;
        match trib_result {
            Ok(value) => Ok(Response::new(StringList { list: value.0 })),
            Err(e) => Err(Status::aborted(format!("{}", e))),
        }
    }
    async fn list_get(
        &self,
        request: tonic::Request<Key>,
    ) -> Result<tonic::Response<StringList>, tonic::Status> {
        let storage = &self.storage;
        let key = request.into_inner();
        let trib_result = storage.list_get(&key.key).await;
        match trib_result {
            Ok(value) => Ok(Response::new(StringList { list: value.0 })),
            Err(e) => Err(Status::aborted(format!("{}", e))),
        }
    }
    async fn list_append(
        &self,
        request: tonic::Request<KeyValue>,
    ) -> Result<tonic::Response<Bool>, tonic::Status> {
        let storage = &self.storage;
        let key_value = request.into_inner();
        let trib_result = storage
            .list_append(&storage::KeyValue {
                key: key_value.key,
                value: key_value.value,
            })
            .await;
        match trib_result {
            Ok(value) => Ok(Response::new(Bool { value })),
            Err(e) => Err(Status::aborted(format!("{}", e))),
        }
    }
    async fn list_remove(
        &self,
        request: tonic::Request<KeyValue>,
    ) -> Result<tonic::Response<ListRemoveResponse>, tonic::Status> {
        let storage = &self.storage;
        let key_value = request.into_inner();
        let trib_result = storage
            .list_remove(&storage::KeyValue {
                key: key_value.key,
                value: key_value.value,
            })
            .await;
        match trib_result {
            Ok(value) => Ok(Response::new(ListRemoveResponse { removed: value })),
            Err(e) => Err(Status::aborted(format!("{}", e))),
        }
    }
    async fn list_keys(
        &self,
        request: tonic::Request<Pattern>,
    ) -> Result<tonic::Response<StringList>, tonic::Status> {
        let storage = &self.storage;
        let pattern = request.into_inner();
        let trib_result = storage
            .list_keys(&storage::Pattern {
                prefix: pattern.prefix,
                suffix: pattern.suffix,
            })
            .await;
        match trib_result {
            Ok(value) => Ok(Response::new(StringList { list: value.0 })),
            Err(e) => Err(Status::aborted(format!("{}", e))),
        }
    }
    async fn clock(
        &self,
        request: tonic::Request<Clock>,
    ) -> Result<tonic::Response<Clock>, tonic::Status> {
        let storage = &self.storage;
        let clock = request.into_inner();
        let trib_result = storage.clock(clock.timestamp).await;
        match trib_result {
            Ok(value) => Ok(Response::new(Clock { timestamp: value })),
            Err(e) => Err(Status::aborted(format!("{}", e))),
        }
    }
}
