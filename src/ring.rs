use std::sync::Arc;

use crate::Result;
use tonic::codegen::http::Uri;

use super::replication::Backend;

/// FNV-1a followed by a fixed avalanche mixes similar host/port strings well.
/// Every process uses the same specified mapping, including after restart.
pub fn hash(value: &str) -> u64 {
    let mut hash = value
        .as_bytes()
        .iter()
        .fold(0xcbf29ce484222325, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        });
    hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d049bb133111eb);
    hash ^ (hash >> 31)
}

pub fn normalize_address(address: &str) -> Result<String> {
    let address = address.trim().trim_end_matches('/');
    let url = if address.contains("://") {
        address.to_owned()
    } else {
        format!("http://{address}")
    };
    let uri: Uri = url.parse()?;
    let scheme = uri.scheme_str().ok_or("backend address has no scheme")?;
    if scheme != "http" || uri.path() != "/" || uri.query().is_some() {
        return Err("backend address must be a plaintext HTTP host and port".into());
    }
    let authority = uri.authority().ok_or("backend address has no host")?;
    if authority.port_u16().is_none() || authority.as_str().contains('@') {
        return Err("backend address must include a port".into());
    }
    Ok(format!("{scheme}://{}", authority.as_str().to_lowercase()))
}

pub struct Ring {
    pub nodes: Vec<Arc<Backend>>,
    positions: Vec<u64>,
}

impl Ring {
    pub fn new(addresses: Vec<String>) -> Result<Self> {
        let mut addresses = addresses
            .iter()
            .map(|a| normalize_address(a))
            .collect::<Result<Vec<_>>>()?;
        addresses.sort();
        addresses.dedup();
        if addresses.is_empty() {
            return Err("at least one backend address is required".into());
        }
        addresses
            .sort_by_key(|address| (hash(address.split_once("://").unwrap().1), address.clone()));
        let positions = addresses
            .iter()
            .map(|a| hash(a.split_once("://").unwrap().1))
            .collect();
        let nodes = addresses
            .into_iter()
            .enumerate()
            .map(|(slot, address)| Arc::new(Backend::new(address, slot as u64)))
            .collect();
        Ok(Self { nodes, positions })
    }

    /// Walk the configured ring once; callers skip failed RPCs themselves.
    pub fn clockwise<'a>(&'a self, bin: &str) -> impl Iterator<Item = &'a Arc<Backend>> {
        let position = hash(bin);
        let start = self.positions.partition_point(|p| *p < position) % self.nodes.len();
        (0..self.nodes.len()).map(move |offset| &self.nodes[(start + offset) % self.nodes.len()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_membership_fails_before_requests_start() {
        assert!(Ring::new(Vec::new()).is_err());
        for address in [
            "localhost",
            "https://localhost:1",
            "localhost:1/path",
            "http://localhost:1?x=1",
            "http://user@localhost:1",
        ] {
            assert!(Ring::new(vec![address.into()]).is_err(), "{address}");
        }
    }

    #[test]
    fn stable_ring_normalizes_and_visits_distinct_addresses() {
        assert_eq!(hash("hello"), 0x16fe05a1c75bcd0f);
        let addresses = ["LOCALHOST:1001", "http://localhost:1001/", "localhost:1002"];
        let ring = Ring::new(addresses.iter().map(|a| a.to_string()).collect()).unwrap();
        assert_eq!(ring.nodes.len(), 2);
        let order: Vec<_> = ring.clockwise("alice").map(|n| n.address.clone()).collect();
        let reversed = Ring::new(vec!["localhost:1002".into(), "localhost:1001".into()]).unwrap();
        assert_eq!(
            order,
            reversed
                .clockwise("alice")
                .map(|n| n.address.clone())
                .collect::<Vec<_>>()
        );
        assert_ne!(order[0], order[1]);
    }
}
