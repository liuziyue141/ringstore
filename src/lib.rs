//! Replicated scalar and list storage with independently computed placement.
//!
//! Start backends with [`backend::serve_back`], optionally start repair workers
//! with [`serve_keeper`], and construct a [`Client`] from the backend addresses.
//! See the repository's architecture document for the supported failure model.

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub mod backend;
mod bin;
mod client;
pub mod config;
pub mod keeper;
pub mod ring;
pub mod storage;

#[doc(hidden)]
pub mod colon;
#[doc(hidden)]
pub mod key_codec;
mod operation;
mod replication;
mod service;

#[doc(hidden)]
// Tonic's generated RPC signatures require its unboxed Status error type.
#[allow(clippy::result_large_err)]
pub mod rpc {
    pub mod storage {
        tonic::include_proto!("storage");
    }
    pub mod control {
        tonic::include_proto!("control");
    }
}

pub use client::Client;
pub use service::{new_bin_client, serve_keeper};
