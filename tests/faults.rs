use std::{
    collections::HashSet,
    io::{BufRead, BufReader, Write},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use ringstore::{backend, new_bin_client, ring::Ring, serve_keeper};
use ringstore::{
    colon::escape,
    config::{BackendConfig, Config, KeeperConfig},
    storage::{BinStorage, KeyList, KeyString, KeyValue, List, MemStorage, Pattern, Storage},
    Result,
};
use tokio::{
    sync::Notify,
    task::JoinSet,
    time::{sleep, timeout},
};

const DEADLINE: Duration = Duration::from_secs(15);
const RELEASE_COPY: &str = "__test_release_copy";

/// Child processes give crash tests real connection loss and complete memory loss.
/// Invoked by the parent test harness with --exact, not by ordinary test runs.
#[test]
fn service_process_fixture() {
    let Ok(role) = std::env::var("RINGSTORE_TEST_ROLE") else {
        return;
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (ready, receiver) = std::sync::mpsc::channel();
    let task = match role.as_str() {
        "backend" => {
            let address = std::env::var("RINGSTORE_TEST_ADDRESS").unwrap();
            let storage = PausedCopyStorage {
                inner: MemStorage::new(),
                pause_key: std::env::var("RINGSTORE_TEST_PAUSE_KEY").ok(),
                copies: AtomicUsize::new(0),
                paused: AtomicBool::new(true),
                release: Notify::new(),
            };
            runtime.spawn(backend::serve_back(BackendConfig {
                address,
                storage: Box::new(storage),
                ready: Some(ready),
                shutdown: None,
            }))
        }
        "keeper" => {
            let config: Config =
                serde_json::from_str(&std::env::var("RINGSTORE_TEST_CONFIG").unwrap()).unwrap();
            let index = std::env::var("RINGSTORE_TEST_INDEX")
                .unwrap()
                .parse()
                .unwrap();
            runtime.spawn(serve_keeper(KeeperConfig {
                backends: config.backends,
                keepers: config.keepers,
                index,
                incarnation: rand::random::<u128>().max(1),
                ready: Some(ready),
                shutdown: None,
            }))
        }
        _ => panic!("unknown fixture role"),
    };
    assert!(receiver.recv_timeout(DEADLINE).unwrap());
    println!("READY");
    std::io::stdout().flush().unwrap();
    runtime.block_on(task).unwrap().unwrap();
}

/// A deterministic migration barrier: the destination accepts one entry and
/// blocks subsequent copies until the parent releases it through a raw RPC.
struct PausedCopyStorage {
    inner: MemStorage,
    pause_key: Option<String>,
    copies: AtomicUsize,
    paused: AtomicBool,
    release: Notify,
}

#[async_trait]
impl KeyString for PausedCopyStorage {
    async fn get(&self, key: &str) -> Result<Option<String>> {
        self.inner.get(key).await
    }
    async fn set(&self, kv: &KeyValue) -> Result<bool> {
        if kv.key == RELEASE_COPY {
            self.paused.store(false, Ordering::SeqCst);
            self.release.notify_waiters();
            return Ok(true);
        }
        self.inner.set(kv).await
    }
    async fn keys(&self, pattern: &Pattern) -> Result<List> {
        self.inner.keys(pattern).await
    }
}

#[async_trait]
impl KeyList for PausedCopyStorage {
    async fn list_get(&self, key: &str) -> Result<List> {
        self.inner.list_get(key).await
    }
    async fn list_append(&self, kv: &KeyValue) -> Result<bool> {
        if self.pause_key.as_deref() == Some(&kv.key)
            && self.copies.fetch_add(1, Ordering::SeqCst) > 0
        {
            loop {
                let notified = self.release.notified();
                if !self.paused.load(Ordering::SeqCst) {
                    break;
                }
                notified.await;
            }
        }
        self.inner.list_append(kv).await
    }
    async fn list_remove(&self, kv: &KeyValue) -> Result<u32> {
        self.inner.list_remove(kv).await
    }
    async fn list_keys(&self, pattern: &Pattern) -> Result<List> {
        self.inner.list_keys(pattern).await
    }
}

#[async_trait]
impl Storage for PausedCopyStorage {
    async fn clock(&self, at_least: u64) -> Result<u64> {
        self.inner.clock(at_least).await
    }
}

struct Running(Child);
impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn start_process(mut command: Command) -> Result<Running> {
    let mut process = Running(
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?,
    );
    let output = process.0.stdout.take().unwrap();
    timeout(
        DEADLINE,
        tokio::task::spawn_blocking(move || {
            for line in BufReader::new(output).lines() {
                if line?.ends_with("READY") {
                    return Ok::<_, std::io::Error>(());
                }
            }
            Err(std::io::Error::other("fixture exited before readiness"))
        }),
    )
    .await???;
    Ok(process)
}

fn fixture_command(role: &str) -> Result<Command> {
    let mut command = Command::new(std::env::current_exe()?);
    command.args(["--exact", "service_process_fixture", "--nocapture"]);
    command.env("RINGSTORE_TEST_ROLE", role);
    Ok(command)
}

struct System {
    config: Config,
    backends: Vec<Option<Running>>,
    keepers: Vec<Option<Running>>,
    ports: Vec<Option<tokio::net::TcpSocket>>,
}

impl System {
    fn new(backend_count: usize, keeper_count: usize) -> Result<Self> {
        // Bound, non-listening sockets reserve inactive addresses without making
        // them appear reachable to the frontend's TCP probes.
        let ports: Vec<_> = (0..backend_count + keeper_count)
            .map(|_| {
                let socket = tokio::net::TcpSocket::new_v4()?;
                socket.bind("127.0.0.1:0".parse().unwrap())?;
                Ok::<_, std::io::Error>(socket)
            })
            .collect::<Result<_, _>>()?;
        let addresses: Vec<_> = ports
            .iter()
            .map(|l| Ok(l.local_addr()?.to_string()))
            .collect::<Result<_, std::io::Error>>()?;
        Ok(Self {
            config: Config {
                backends: addresses[..backend_count].to_vec(),
                keepers: addresses[backend_count..].to_vec(),
            },
            backends: (0..backend_count).map(|_| None).collect(),
            keepers: (0..keeper_count).map(|_| None).collect(),
            ports: ports.into_iter().map(Some).collect(),
        })
    }

    async fn backend(&mut self, index: usize, pause_key: Option<&str>) -> Result<()> {
        let mut command = fixture_command("backend")?;
        command.env("RINGSTORE_TEST_ADDRESS", &self.config.backends[index]);
        if let Some(key) = pause_key {
            command.env("RINGSTORE_TEST_PAUSE_KEY", key);
        }
        self.ports[index].take();
        self.backends[index] = Some(start_process(command).await?);
        Ok(())
    }

    async fn keeper(&mut self, index: usize) -> Result<()> {
        let mut command = fixture_command("keeper")?;
        command.env(
            "RINGSTORE_TEST_CONFIG",
            serde_json::to_string(&self.config)?,
        );
        command.env("RINGSTORE_TEST_INDEX", index.to_string());
        self.ports[self.backends.len() + index].take();
        self.keepers[index] = Some(start_process(command).await?);
        Ok(())
    }

    async fn all_backends(&mut self) -> Result<()> {
        for index in 0..self.backends.len() {
            self.backend(index, None).await?;
        }
        Ok(())
    }

    async fn client(&self) -> Result<Box<dyn BinStorage>> {
        new_bin_client(self.config.backends.clone()).await
    }

    async fn raw(&self, index: usize) -> Result<Box<dyn Storage>> {
        backend::new_client(&format!("http://{}", self.config.backends[index])).await
    }

    async fn holders(&self, key: &str) -> Result<Vec<usize>> {
        let mut holders = Vec::new();
        for index in 0..self.backends.len() {
            if self.backends[index].is_some()
                && !self.raw(index).await?.list_get(key).await?.0.is_empty()
            {
                holders.push(index);
            }
        }
        Ok(holders)
    }

    fn stop_backend(&mut self, index: usize) -> Result<()> {
        self.backends[index].take();
        let socket = tokio::net::TcpSocket::new_v4()?;
        socket.set_reuseaddr(true)?;
        socket.bind(self.config.backends[index].parse()?)?;
        self.ports[index] = Some(socket);
        Ok(())
    }
    fn stop_keeper(&mut self, index: usize) -> Result<()> {
        self.keepers[index].take();
        let socket = tokio::net::TcpSocket::new_v4()?;
        socket.set_reuseaddr(true)?;
        socket.bind(self.config.keepers[index].parse()?)?;
        self.ports[self.backends.len() + index] = Some(socket);
        Ok(())
    }
}

fn log_key(bin: &str, kind: &str, key: &str) -> String {
    format!("ringstore::v1::{}::{kind}::{}", escape(bin), escape(key))
}

async fn wait_for_log(system: &System, index: usize, key: &str, count: usize) -> Result<()> {
    timeout(DEADLINE, async {
        loop {
            if system.raw(index).await?.list_get(key).await?.0.len() >= count {
                return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_live_backend_without_keeper_supports_all_storage_calls() -> Result<()> {
    let mut system = System::new(4, 0)?;
    system.backend(3, None).await?;
    let client = system.client().await?;
    let bin = client.bin("::||bin").await?;
    timeout(DEADLINE, async {
        assert_eq!(bin.get("").await?, None);
        assert!(bin.list_get("").await?.0.is_empty());
        bin.set(&KeyValue::new("a:|;", "scalar")).await?;
        bin.list_append(&KeyValue::new("a:|;", "list")).await?;
        assert_eq!(bin.get("a:|;").await?, Some("scalar".into()));
        assert_eq!(bin.list_get("a:|;").await?.0, vec!["list"]);
        let pattern = Pattern {
            prefix: "a:".into(),
            suffix: "|;".into(),
        };
        assert_eq!(bin.keys(&pattern).await?.0, vec!["a:|;"]);
        assert_eq!(bin.list_keys(&pattern).await?.0, vec!["a:|;"]);
        bin.set(&KeyValue::new("a:|;", "")).await?;
        assert_eq!(bin.get("a:|;").await?, None);
        assert!(bin.keys(&Pattern::default()).await?.0.is_empty());
        assert_eq!(bin.list_remove(&KeyValue::new("a:|;", "list")).await?, 1);
        assert!(bin.list_keys(&Pattern::default()).await?.0.is_empty());
        let first = bin.clock(1000).await?;
        assert!(first >= 1000);
        assert!(bin.clock(0).await? > first);
        assert!(client.bin("::||bin2").await?.get("a:|;").await?.is_none());
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_replicate_to_three_distinct_nodes_and_preserve_duplicate_values() -> Result<()> {
    let mut system = System::new(4, 0)?;
    system.all_backends().await?;
    let client = system.client().await?;
    let bin = client.bin("alice").await?;
    bin.list_append(&KeyValue::new("k", "same")).await?;
    bin.list_append(&KeyValue::new("k", "same")).await?;
    let key = log_key("alice", "list", "k");
    let holders = system.holders(&key).await?;
    assert_eq!(holders.len(), 3);
    let raw = system.raw(holders[0]).await?;
    let entry = raw.list_get(&key).await?.0[0].clone();
    raw.list_append(&KeyValue::new(&key, &entry)).await?;
    assert_eq!(bin.list_get("k").await?.0, vec!["same", "same"]);
    assert_eq!(bin.list_remove(&KeyValue::new("k", "same")).await?, 2);
    assert_eq!(bin.list_remove(&KeyValue::new("k", "same")).await?, 0);
    // Re-delivering an old append cannot resurrect a removed occurrence.
    raw.list_append(&KeyValue::new(&key, &entry)).await?;
    assert!(bin.list_get("k").await?.0.is_empty());
    bin.list_append(&KeyValue::new("k", "same")).await?;
    assert_eq!(bin.list_get("k").await?.0, vec!["same"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_live_backends_receive_two_copies() -> Result<()> {
    let mut system = System::new(4, 0)?;
    system.backend(1, None).await?;
    system.backend(3, None).await?;
    let client = system.client().await?;
    client
        .bin("alice")
        .await?
        .set(&KeyValue::new("k", "value"))
        .await?;
    assert_eq!(
        system.holders(&log_key("alice", "scalar", "k")).await?,
        vec![1, 3]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_merge_disjoint_histories_and_repair_scalar_values() -> Result<()> {
    let mut system = System::new(4, 0)?;
    system.all_backends().await?;
    let key = log_key("alice", "list", "k");
    for (index, id, timestamp, value) in [(0, "a", 1, "one"), (1, "b", 2, "two")] {
        let entry =
            serde_json::json!({"id": id, "timestamp": timestamp, "action": {"Append": value}})
                .to_string();
        system
            .raw(index)
            .await?
            .list_append(&KeyValue::new(&key, &entry))
            .await?;
    }
    let scalar = log_key("alice", "scalar", "k");
    for (index, id, timestamp, value) in [(0, "s1", 1, "old"), (1, "s2", 2, "new")] {
        let entry = serde_json::json!({"id": id, "timestamp": timestamp, "action": {"Set": value}})
            .to_string();
        system
            .raw(index)
            .await?
            .list_append(&KeyValue::new(&scalar, &entry))
            .await?;
    }
    let client = system.client().await?;
    let bin = client.bin("alice").await?;
    assert_eq!(bin.list_get("k").await?.0, vec!["one", "two"]);
    assert_eq!(bin.get("k").await?, Some("new".into()));
    let mut copies = 0;
    for index in 0..4 {
        copies += usize::from(
            system
                .raw(index)
                .await?
                .list_get(&scalar)
                .await?
                .0
                .iter()
                .any(|s| s.contains("s2")),
        );
    }
    assert!(copies >= 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_clocks_are_unique_and_survive_allocator_restart() -> Result<()> {
    let mut system = System::new(4, 0)?;
    system.all_backends().await?;
    let mut tasks = JoinSet::new();
    for _ in 0..64 {
        let addresses = system.config.backends.clone();
        tasks.spawn(async move {
            new_bin_client(addresses)
                .await?
                .bin("alice")
                .await?
                .clock(1000)
                .await
        });
    }
    let mut values = HashSet::new();
    timeout(DEADLINE, async {
        while let Some(result) = tasks.join_next().await {
            assert!(values.insert(result??));
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await??;
    assert_eq!(values.len(), 64);
    let maximum = *values.iter().max().unwrap();
    let ring = Ring::new(system.config.backends.clone())?;
    let allocator = &ring.clockwise("__ringstore_clock").next().unwrap().address;
    let index = system
        .config
        .backends
        .iter()
        .position(|a| format!("http://{a}") == *allocator)
        .unwrap();
    system.stop_backend(index)?;
    let client = system.client().await?;
    assert!(client.bin("alice").await?.clock(0).await? > maximum);
    system.backend(index, None).await?;
    let next = client.bin("alice").await?.clock(0).await?;
    assert!(next > maximum);
    assert!(client.bin("bob").await?.clock(next + 1).await? > next);
    assert_eq!(client.bin("alice").await?.clock(u64::MAX).await?, u64::MAX);
    assert_eq!(client.bin("alice").await?.clock(0).await?, u64::MAX);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keeper_restarts_partial_migration_by_operation_identity() -> Result<()> {
    let mut system = System::new(4, 2)?;
    for index in 0..3 {
        system.backend(index, None).await?;
    }
    let ring = Ring::new(system.config.backends.clone())?;
    let new_address = format!("http://{}", system.config.backends[3]);
    let bin_name = (0..10000)
        .map(|n| format!("bin{n}"))
        .find(|name| {
            ring.clockwise(name)
                .take(3)
                .any(|node| node.address == new_address)
        })
        .unwrap();
    let client = system.client().await?;
    let bin = client.bin(&bin_name).await?;
    for value in ["one", "two", "three"] {
        bin.list_append(&KeyValue::new("k", value)).await?;
    }
    let key = log_key(&bin_name, "list", "k");
    system.keeper(0).await?;
    system.backend(3, Some(&key)).await?;
    wait_for_log(&system, 3, &key, 1).await?;
    system.stop_keeper(0)?; // No cleanup or keeper-local migration state survives.
    system
        .raw(3)
        .await?
        .set(&KeyValue::new(RELEASE_COPY, "yes"))
        .await?;
    system.keeper(1).await?;
    wait_for_log(&system, 3, &key, 3).await?;
    assert_eq!(bin.list_get("k").await?.0, vec!["one", "two", "three"]);
    // Repeated reconciliation does not invent additional list occurrences.
    let keeper = ringstore::keeper::RepairKeeper::new(
        system.config.backends.clone(),
        system.config.keepers.clone(),
        1,
        123,
    )?;
    keeper.reconcile().await?;
    assert_eq!(bin.list_get("k").await?.0.len(), 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_keeper_repairs_fresh_backends_after_primary_crash() -> Result<()> {
    let mut system = System::new(5, 2)?;
    system.all_backends().await?;
    system.keeper(0).await?;
    system.keeper(1).await?;
    let client = system.client().await?;
    let bin = client.bin("alice").await?;
    bin.list_append(&KeyValue::new("k", "kept")).await?;
    let key = log_key("alice", "list", "k");
    let holder = system.holders(&key).await?[0];
    system.stop_keeper(0)?;
    system.stop_backend(holder)?;
    assert_eq!(bin.list_get("k").await?.0, vec!["kept"]);
    system.backend(holder, None).await?;
    wait_for_log(&system, holder, &key, 1).await?;
    assert_eq!(bin.list_get("k").await?.0, vec!["kept"]);
    // Another backend crash after completed repair still leaves the data intact.
    let second = system
        .holders(&key)
        .await?
        .into_iter()
        .find(|i| *i != holder)
        .unwrap();
    system.stop_backend(second)?;
    assert_eq!(bin.list_get("k").await?.0, vec!["kept"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_and_deletions_continue_during_backend_failure() -> Result<()> {
    let mut system = System::new(5, 1)?;
    system.all_backends().await?;
    system.keeper(0).await?;
    let client = system.client().await?;
    let bin = client.bin("alice").await?;
    bin.set(&KeyValue::new("k", "old")).await?;
    let key = log_key("alice", "scalar", "k");
    let holder = system.holders(&key).await?[0];
    system.stop_backend(holder)?;
    bin.set(&KeyValue::new("k", "new")).await?;
    assert_eq!(bin.get("k").await?, Some("new".into()));
    bin.set(&KeyValue::new("k", "")).await?;
    system.backend(holder, None).await?;
    assert_eq!(bin.get("k").await?, None);
    assert!(bin.keys(&Pattern::default()).await?.0.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_appends_have_one_final_order_across_clients() -> Result<()> {
    let mut system = System::new(4, 0)?;
    system.all_backends().await?;
    let mut tasks = JoinSet::new();
    for index in 0..24 {
        let addresses = system.config.backends.clone();
        tasks.spawn(async move {
            let client = new_bin_client(addresses).await?;
            client
                .bin("alice")
                .await?
                .list_append(&KeyValue::new("k", &(index % 3).to_string()))
                .await
        });
    }
    timeout(DEADLINE, async {
        while let Some(result) = tasks.join_next().await {
            assert!(result??);
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await??;
    let first = system
        .client()
        .await?
        .bin("alice")
        .await?
        .list_get("k")
        .await?
        .0;
    let second = system
        .client()
        .await?
        .bin("alice")
        .await?
        .list_get("k")
        .await?
        .0;
    assert_eq!(first, second);
    assert_eq!(first.len(), 24);
    for value in ["0", "1", "2"] {
        assert_eq!(first.iter().filter(|v| *v == value).count(), 8);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clock_calls_in_flight_during_crash_and_restart_do_not_collide() -> Result<()> {
    let mut system = System::new(4, 0)?;
    system.all_backends().await?;
    let ring = Ring::new(system.config.backends.clone())?;
    let allocator = &ring.clockwise("__ringstore_clock").next().unwrap().address;
    let index = system
        .config
        .backends
        .iter()
        .position(|a| format!("http://{a}") == *allocator)
        .unwrap();
    let mut tasks = JoinSet::new();
    for _ in 0..64 {
        let addresses = system.config.backends.clone();
        tasks.spawn(async move {
            new_bin_client(addresses)
                .await?
                .bin("alice")
                .await?
                .clock(0)
                .await
        });
    }
    let first = timeout(DEADLINE, tasks.join_next()).await?.unwrap()??;
    system.stop_backend(index)?;
    system.backend(index, None).await?;
    let mut values = HashSet::from([first]);
    timeout(DEADLINE, async {
        while let Some(result) = tasks.join_next().await {
            assert!(values.insert(result??));
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await??;
    assert_eq!(values.len(), 64);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_live_backends_in_a_300_address_configuration_are_usable() -> Result<()> {
    let mut system = System::new(300, 0)?;
    for index in [0, 149, 299] {
        system.backend(index, None).await?;
    }
    let client = system.client().await?;
    let bin = client.bin("alice").await?;
    timeout(DEADLINE, async {
        bin.list_append(&KeyValue::new("k", "value")).await?;
        assert_eq!(bin.list_get("k").await?.0, vec!["value"]);
        assert_eq!(
            system.holders(&log_key("alice", "list", "k")).await?,
            vec![0, 149, 299]
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_patterns_match_decoded_characters_without_escape_boundary_matches() -> Result<()> {
    let mut system = System::new(1, 0)?;
    system.all_backends().await?;
    let client = system.client().await?;
    let bin = client.bin("").await?;
    for key in ["ends|;", "ends:", ""] {
        bin.set(&KeyValue::new(key, "value")).await?;
        bin.list_append(&KeyValue::new(key, "value")).await?;
    }
    let pattern = Pattern {
        prefix: String::new(),
        suffix: ":".into(),
    };
    assert_eq!(bin.keys(&pattern).await?.0, vec!["ends:"]);
    assert_eq!(bin.list_keys(&pattern).await?.0, vec!["ends:"]);
    assert_eq!(bin.get("").await?, Some("value".into()));
    assert_eq!(bin.list_get("").await?.0, vec!["value"]);
    assert!(client
        .bin("other")
        .await?
        .keys(&Pattern::default())
        .await?
        .0
        .is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_mutations_still_advance_after_the_public_clock_saturates() -> Result<()> {
    let mut system = System::new(3, 0)?;
    system.all_backends().await?;
    let client = system.client().await?;
    let bin = client.bin("alice").await?;
    assert_eq!(bin.clock(u64::MAX).await?, u64::MAX);
    bin.set(&KeyValue::new("k", "old")).await?;
    bin.set(&KeyValue::new("k", "new")).await?;
    assert_eq!(bin.get("k").await?, Some("new".into()));
    bin.set(&KeyValue::new("k", "")).await?;
    assert_eq!(bin.get("k").await?, None);
    bin.list_append(&KeyValue::new("k", "one")).await?;
    bin.list_append(&KeyValue::new("k", "two")).await?;
    assert_eq!(bin.list_get("k").await?.0, vec!["one", "two"]);
    assert_eq!(bin.list_remove(&KeyValue::new("k", "one")).await?, 1);
    assert_eq!(bin.list_get("k").await?.0, vec!["two"]);
    Ok(())
}
