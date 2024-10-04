//! # SWIM Cluster Module
//!
//! This module provides a high-level API for creating and managing SWIM cluster nodes.
//! It encapsulates the complexity of setting up and running a SWIM node,
//! offering an easy-to-use interface through the [`SwimCluster`] struct.
//!
//! ## Overview
//!
//! The SWIM protocol is designed for scalable and efficient cluster membership and failure detection.
//! The [`SwimCluster`] struct allows you to initialize a SWIM node, configure it,
//! and run it within a cluster. It handles underlying networking,
//! failure detection, and membership protocols as described in the SWIM protocol paper.
//!
//! ## Example
//!
//! ```rust,no_run
//! use swim_rs::{api::{config::SwimConfig, swim::SwimCluster}, Result};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Create a new SWIM cluster node with default configuration
//!     let cluster = SwimCluster::try_new("127.0.0.1:8080", SwimConfig::new()).await?;
//!
//!     // Run the SWIM node
//!     cluster.run().await;
//!
//!     // Your application logic here
//!
//!     Ok(())
//! }
//! ```
//! ## Usage
//!
//! To use this module, import the [`SwimCluster`] struct and create a new instance using [`try_new`].
//! Then, call [`run`] to start the node. You can subscribe to events and access the membership list as needed.
//!
//! For detailed configuration options, refer to the [`config`] module.
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::sync::broadcast::{self, Receiver};
use tokio::task::JoinHandle;

use crate::api::init_tracing;
use crate::core::member::MembershipList;
use crate::core::node::SwimNode;
use crate::error::Result;
use crate::pb::gossip::Event;

use super::config::SwimConfig;

/// A high-level API for creating and managing a SWIM cluster node.
///
/// `SwimCluster` provides methods to initialize, configure, and run a SWIM node
/// within a cluster. It encapsulates lower-level details and exposes a simplified interface.
#[derive(Clone, Debug)]
pub struct SwimCluster {
    /// The `SwimNode` within the cluster.
    node: Arc<SwimNode<UdpSocket>>,
}

impl SwimCluster {
    /// Creates a new [`SwimCluster`] node bound to the specified address with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `addr` - The local address to bind the node to (e.g., "127.0.0.1:8080").
    /// * `config` - The [`SwimConfig`] containing protocol settings.
    ///
    /// # Returns
    ///
    /// A `Result` containing the `SwimCluster` instance or an error if initialization fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use swim_rs::{api::{config::SwimConfig, swim::SwimCluster},Result};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let config = SwimConfig::new();
    ///     let cluster = SwimCluster::try_new("127.0.0.1:8080", config).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn try_new(addr: impl AsRef<str>, config: SwimConfig) -> Result<Self> {
        let socket = UdpSocket::bind(addr.as_ref()).await?;
        let (tx, _) = broadcast::channel::<Event>(32);
        let node = Arc::new(SwimNode::try_new(socket, config, tx).await?);

        Ok(Self { node })
    }

    /// Returns the local address of the node.
    pub fn addr(&self) -> &str {
        self.node.addr()
    }

    /// Returns a reference to the node's configuration.
    pub fn config(&self) -> &SwimConfig {
        self.node.config()
    }

    /// Subscribes to events emitted by the node.
    ///
    /// Returns a `Receiver<Event>` that can be used to receive events such as
    /// node joins, departures, and state changes.
    pub fn subscribe(&self) -> Receiver<Event> {
        self.node.subscribe()
    }

    /// Returns a reference to the node's membership list.
    ///
    /// The membership list contains information about all known nodes in the cluster.
    pub fn membership_list(&self) -> &MembershipList {
        self.node.membership_list()
    }

    /// Starts the SWIM protocol and runs the node.
    ///
    /// This method initializes tracing, logs the startup message, and starts the node's
    /// background tasks. It returns join handles for the tasks, which can be awaited or managed.
    ///
    /// # Returns
    ///
    /// A tuple containing two `JoinHandle<()>` instances corresponding to the node's background tasks:
    /// * failure detection and
    /// * message handling
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use swim_rs::{api::{config::SwimConfig, swim::SwimCluster}, Result};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let config = SwimConfig::new();
    ///     let cluster = SwimCluster::try_new("127.0.0.1:8080", config).await?;
    ///     cluster.run().await;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn run(&self) -> (JoinHandle<()>, JoinHandle<()>) {
        init_tracing();

        tracing::info!("[{}] starting SwimNode...", self.node.addr());
        self.node.run().await
    }
}
