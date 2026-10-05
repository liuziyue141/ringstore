//! Ephemeral backend service and a raw client used for diagnosis and fault tests.

mod client;
mod server;
mod service;

pub use service::{new_client, serve_back};
