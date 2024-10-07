# swim-rs

`swim-rs` is an implementation of the [SWIM protocol](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) in Rust, designed for efficient and scalable cluster membership and failure detection in distributed systems.

This library is based on the official SWIM protocol paper and includes optimizations such as:

- **Piggybacking Instead of Multicasting**: Reduces network overhead by attaching membership updates to periodic messages rather than sending separate multicast messages.
- **Round-Robin Member Selection**: Ensures uniform probing of cluster members for health checks, improving detection accuracy.
- **Suspicion Mechanism**: Introduces an intermediate state between alive and dead, reducing false positives in failure detection.

## How It Works

`swim-rs` implements the SWIM protocol to manage cluster membership and detect node failures in a distributed system efficiently. At its core, each node periodically selects a random peer to send a PING message, verifying its health and availability. If the selected peer fails to respond within a specified timeframe, the node escalates the check by sending a PING_REQ to multiple other members of the cluster. This two-tiered approach minimizes false positives in failure detection by confirming suspicions through multiple independent confirmations.

To optimize network usage, swim-rs employs piggybacking, where membership updates and state changes are attached to regular messages (UDP) rather than sending separate multicast messages. Additionally, round-robin member selection ensures that all nodes are probed uniformly, preventing any single node from becoming a bottleneck in the failure detection process. The protocol also incorporates a suspicion mechanism, introducing an intermediate state between alive and dead, which helps in reducing the chances of incorrectly marking healthy nodes as failed.

Furthermore, swim-rs utilizes a gossip-based dissemination strategy to propagate membership information and state transitions across the cluster. This ensures that all nodes maintain a consistent and up-to-date view of the cluster's state, enhancing scalability and resilience.

## Features

- **Efficient Failure Detection**: Implements the SWIM protocol with optimizations for low network overhead and high accuracy.
- **Scalable Membership Management**: Manages cluster membership with support for dynamic node addition and removal.
- **Event-Driven Architecture**: Emits events for significant cluster changes, allowing applications to react accordingly.
- **Asynchronous Operations**: Built with Tokio for high-performance asynchronous networking.

## Installation

Add `swim-rs` to your `Cargo.toml`:

```toml
[dependencies]
swim-rs = "0.1.1"
```

## Basic Example

The following example demonstrates how to create two nodes in the same cluster and run the SWIM protocol:

```rust
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

    // Simulate a long-running process or service
    tokio::time::sleep(Duration::from_secs(12)).await;

    Ok(())
}
```

## Event Subscription Example

The following example demonstrates how to subscribe to events emitted by nodes in the SWIM cluster.

```rust
use std::time::Duration;

use swim_rs::{
    api::{config::SwimConfig, swim::SwimCluster},
    Event, Result,
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

    // Subscribe and receive events from node1
    let mut rx1 = node1.subscribe();

    // Handle events accordingly
    while let Ok(event) = rx1.recv().await {
       match event {
           Event::NodeJoined(e) => tracing::info!("[{}] handle {:#?}", node1.addr(), e),
           Event::NodeSuspected(e) => tracing::info!("[{}] handle {:#?}", node1.addr(), e),
           Event::NodeRecovered(e) => tracing::info!("[{}] handle {:#?}", node1.addr(), e),
           Event::NodeDeceased(e) => tracing::info!("[{}] handle {:#?}", node1.addr(), e),
       }
    }

    // Simulate a long-running process or service
    tokio::time::sleep(Duration::from_secs(12)).await;

    Ok(())
}
```

## Roadmap

The following features are planned for future releases to enhance the functionality, security, and robustness of swim-rs:

- Rate Limiting
- Authentication Checks
- Message Integrity Checks

## Contributing

Contributions are welcome!

Please open issues and submit pull requests for any enhancements or bug fixes.

1. Fork the repository.
2. Create a new branch: git checkout -b feature/YourFeature.
3. Commit your changes: git commit -m 'Add some feature'.
4. Push to the branch: git push origin feature/YourFeature.
5. Open a pull request.

## License

This project ist licensed under the Apache License, Version 2.0

## Learn More

- [SWIM Protocol Paper](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)
- [GitHub Repository](https://github.com/marvinlanhenke/swim-rs)
