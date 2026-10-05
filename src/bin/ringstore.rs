use clap::{Parser, Subcommand};
use ringstore::{
    backend::serve_back,
    config::{BackendConfig, Config, KeeperConfig},
    ring::Ring,
    serve_keeper,
    storage::{BinStorage, KeyValue, MemStorage, Pattern},
    Client, Result,
};

#[derive(Parser)]
#[command(
    version,
    about = "Replicated scalar and list storage with restartable repair"
)]
struct Arguments {
    #[arg(long, global = true, default_value = "config.example.json")]
    config: std::path::PathBuf,
    #[arg(long, global = true, default_value = "demo")]
    bin: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start an empty, in-memory backend at its configured address.
    Backend {
        #[arg(long)]
        index: usize,
    },
    /// Start a restartable repair worker.
    Keeper {
        #[arg(long)]
        index: usize,
    },
    /// Show clockwise placement and reachability for a bin.
    Placement,
    /// Show physical and distinct operation counts on each backend.
    Copies {
        key: String,
        #[arg(long)]
        lists: bool,
    },
    Get {
        key: String,
    },
    /// Set a scalar. An empty value deletes it.
    Set {
        key: String,
        value: String,
    },
    Append {
        key: String,
        value: String,
    },
    List {
        key: String,
    },
    /// Remove observed matching appends and print their count.
    Remove {
        key: String,
        value: String,
    },
    Keys {
        #[arg(long, default_value = "")]
        prefix: String,
        #[arg(long, default_value = "")]
        suffix: String,
        #[arg(long)]
        lists: bool,
    },
    Clock {
        #[arg(default_value_t = 0)]
        at_least: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Arguments::parse();
    let config = Config::read(&args.config)?;
    match args.command {
        Command::Backend { index } => {
            let address = config
                .backends
                .get(index)
                .ok_or("backend index is out of range")?
                .clone();
            let (shutdown, receiver) = tokio::sync::mpsc::channel(1);
            let server = serve_back(BackendConfig {
                address,
                storage: Box::new(MemStorage::new()),
                ready: None,
                shutdown: Some(receiver),
            });
            tokio::pin!(server);
            tokio::select! {
                result = &mut server => result?,
                result = tokio::signal::ctrl_c() => {
                    result?;
                    shutdown.send(()).await?;
                    server.await?;
                }
            }
        }
        Command::Keeper { index } => {
            let (shutdown, receiver) = tokio::sync::mpsc::channel(1);
            let server = serve_keeper(KeeperConfig {
                backends: config.backends,
                keepers: config.keepers,
                index,
                incarnation: rand::random::<u128>(),
                ready: None,
                shutdown: Some(receiver),
            });
            tokio::pin!(server);
            tokio::select! {
                result = &mut server => result?,
                result = tokio::signal::ctrl_c() => {
                    result?;
                    shutdown.send(()).await?;
                    server.await?;
                }
            }
        }
        Command::Copies { key, lists } => {
            let ring = Ring::new(config.backends)?;
            let kind = if lists {
                ringstore::key_codec::Kind::List
            } else {
                ringstore::key_codec::Kind::Scalar
            };
            let key = ringstore::key_codec::encode(&args.bin, kind, &key);
            for node in ring.clockwise(&args.bin) {
                let value = match node.read_raw(&key).await {
                    Ok(entries) => {
                        let mut ids = std::collections::HashSet::new();
                        for entry in &entries {
                            let operation: serde_json::Value = serde_json::from_str(entry)?;
                            ids.insert(
                                operation["id"]
                                    .as_str()
                                    .ok_or("missing operation ID")?
                                    .to_owned(),
                            );
                        }
                        serde_json::json!({"address": node.address, "reachable": true,
                            "physical_entries": entries.len(), "distinct_operations": ids.len()})
                    }
                    Err(_) => serde_json::json!({"address": node.address, "reachable": false}),
                };
                println!("{value}");
            }
        }
        Command::Placement => {
            let ring = Ring::new(config.backends)?;
            let mut selected = 0;
            for (rank, node) in ring.clockwise(&args.bin).enumerate() {
                let live = node.inspect().await.is_ok();
                let target = live && selected < 3;
                selected += usize::from(target);
                println!(
                    "{}",
                    serde_json::json!({"rank": rank, "address": node.address,
                        "reachable": live, "write_target": target})
                );
            }
        }
        command => {
            let client = Client::new(config.backends)?;
            let bin = client.bin(&args.bin).await?;
            let value = match command {
                Command::Get { key } => serde_json::json!(bin.get(&key).await?),
                Command::Set { key, value } => {
                    serde_json::json!(bin.set(&KeyValue { key, value }).await?)
                }
                Command::Append { key, value } => {
                    serde_json::json!(bin.list_append(&KeyValue { key, value }).await?)
                }
                Command::List { key } => serde_json::json!(bin.list_get(&key).await?.0),
                Command::Remove { key, value } => {
                    serde_json::json!(bin.list_remove(&KeyValue { key, value }).await?)
                }
                Command::Keys {
                    prefix,
                    suffix,
                    lists,
                } => {
                    let pattern = Pattern { prefix, suffix };
                    let keys = if lists {
                        bin.list_keys(&pattern).await?
                    } else {
                        bin.keys(&pattern).await?
                    };
                    serde_json::json!(keys.0)
                }
                Command::Clock { at_least } => serde_json::json!(bin.clock(at_least).await?),
                _ => unreachable!("service commands handled above"),
            };
            println!("{value}");
        }
    }
    Ok(())
}
