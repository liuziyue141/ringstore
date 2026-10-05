use ringstore::{
    storage::{BinStorage, KeyValue},
    Client, Result,
};

/// Start the backends from config.example.json before running this example.
#[tokio::main]
async fn main() -> Result<()> {
    let config = ringstore::config::Config::read("config.example.json")?;
    let client = Client::new(config.backends)?;
    let bin = client.bin("example").await?;
    bin.set(&KeyValue::new("status", "ready")).await?;
    bin.list_append(&KeyValue::new("events", "started")).await?;
    println!("status: {:?}", bin.get("status").await?);
    println!("events: {:?}", bin.list_get("events").await?.0);
    Ok(())
}
