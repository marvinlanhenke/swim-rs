//! # swim-rs
//!
//! `swim-rs` is an implementation of the [SWIM protocol](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) in Rust,
//! designed for efficient and scalable cluster membership and failure detection in distributed systems.
//!
//! This library is based on the official SWIM protocol paper and includes optimizations such as:
//!
//! - **Piggybacking Instead of Multicasting**: Reduces network overhead by attaching membership updates to periodic messages rather than sending separate multicast messages.
//! - **Round-Robin Member Selection**: Ensures uniform probing of cluster members for health checks, improving detection accuracy.
//! - **Suspicion Mechanism**: Introduces an intermediate state between alive and dead, reducing false positives in failure detection.
//!
//! ## Basic Example
//!
//! The following example demonstrates how to create two nodes in the same cluster and run the SWIM protocol:
//!
//! ```rust,no_run
//! use std::time::Duration;
//!
//! use swim_rs::{
//!     api::{config::SwimConfig, swim::SwimCluster},
//!     Result,
//! };
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Creates two nodes in the same cluster
//!     let node1 = SwimCluster::try_new("127.0.0.1:8080", SwimConfig::new()).await?;
//!     let node2 = SwimCluster::try_new(
//!         "127.0.0.1:8081",
//!         SwimConfig::builder()
//!             .with_known_peers(["127.0.0.1:8080"])
//!             .build(),
//!     )
//!     .await?;
//!
//!     // Run the SWIM protocol in the background
//!     node1.run().await;
//!     node2.run().await;
//!
//!     // Simulate a long-running process or service
//!     tokio::time::sleep(Duration::from_secs(12)).await;
//!
//!     Ok(())
//! }
//! ```
//! ## Event Subscription Example
//!
//! The following example demonstrates how to subscribe to events emitted by nodes in the SWIM cluster.
//!
//! ```rust,no_run
//! use std::time::Duration;
//!
//! use swim_rs::{
//!     api::{config::SwimConfig, swim::SwimCluster},
//!     Event, Result,
//! };
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Creates two nodes in the same cluster
//!     let node1 = SwimCluster::try_new("127.0.0.1:8080", SwimConfig::new()).await?;
//!     let node2 = SwimCluster::try_new(
//!         "127.0.0.1:8081",
//!         SwimConfig::builder()
//!             .with_known_peers(["127.0.0.1:8080"])
//!             .build(),
//!     )
//!     .await?;
//!
//!     // Run the SWIM protocol in the background
//!     node1.run().await;
//!     node2.run().await;
//!
//!     // Subscribe and receive events from node1
//!     let mut rx1 = node1.subscribe();
//!
//!     // Handle events accordingly
//!     while let Ok(event) = rx1.recv().await {
//!        match event {
//!            Event::NodeJoined(e) => tracing::info!("[{}] handle {:#?}", node1.addr(), e),
//!            Event::NodeSuspected(e) => tracing::info!("[{}] handle {:#?}", node1.addr(), e),
//!            Event::NodeRecovered(e) => tracing::info!("[{}] handle {:#?}", node1.addr(), e),
//!            Event::NodeDeceased(e) => tracing::info!("[{}] handle {:#?}", node1.addr(), e),
//!        }
//!     }
//!
//!     // Simulate a long-running process or service
//!     tokio::time::sleep(Duration::from_secs(12)).await;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Learn More
//!
//! - [SWIM Protocol Paper](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)
//! - [GitHub Repository](https://github.com/marvinlanhenke/swim-rs)
pub mod api;

mod core;

mod error;
pub use error::Result;

#[cfg(test)]
#[path = "./test-utils/mod.rs"]
#[doc(hidden)]
mod test_utils;

mod pb {
    include!(concat!(env!("OUT_DIR"), "/v1.swim.rs"));
}
pub use pb::gossip::Event;
pub use pb::gossip::{NodeDeceased, NodeJoined, NodeRecovered, NodeSuspected};
pub use pb::NodeState;
