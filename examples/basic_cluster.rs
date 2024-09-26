use std::time::Duration;

use swim_rs::{
    api::{config::SwimConfig, swim::SwimCluster},
    Result,
};

#[tokio::main]
async fn main() -> Result<()> {
    // Creates two nodes in the same cluster
    let node1 = SwimCluster::try_new("127.0.0.1:8080", SwimConfig::new()).await?;
    let node2 = SwimCluster::try_new(
        "127.0.0.1:8081",
        SwimConfig::builder()
            .with_known_peers(["127.0.0.1:8080"])
            .build(),
    )
    .await?;

    // Run the SWIM protocol in the background
    node1.run().await;
    node2.run().await;

    // Simulate a long running process or service
    tokio::time::sleep(Duration::from_secs(12)).await;

    Ok(())
}
