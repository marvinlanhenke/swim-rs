//! # SWIM Configuration Module
//!
//! The `config` module provides structures and builders for configuring the SWIM protocol node.
//! It allows you to customize various parameters such as timeouts, intervals, and known peers,
//! enabling fine-tuning of the SWIM protocol to suit your application's needs.
use std::time::Duration;

/// Default interval between each PING message sent by a node.
/// This determines how frequently the node checks the health of other nodes.
const DEFAULT_PING_INTERVAL: Duration = Duration::from_millis(3000);

/// Default timeout for a PING message.
/// If no ACK is received within this duration, the node is considered a `Suspect`.
const DEFAULT_PING_TIMEOUT: Duration = Duration::from_millis(1000);

/// Default number of nodes included in a PING-REQ request.
/// This determines how many nodes are asked to confirm the status of a suspected node.
const DEFAULT_PING_REQ_GROUP_SIZE: usize = 1;

/// Default timeout for a PING-REQ message.
/// If no ACK is received within this time, the suspected node is still considered unreachable.
const DEFAULT_PING_REQ_TIMEOUT: Duration = Duration::from_millis(1000);

/// Default timeout until a suspected node is declared as deceased.
/// This is the duration a node remains in the `Suspect` state before being considered dead.
const DEFAULT_SUSPECT_TIMEOUT: Duration = Duration::from_millis(6000);

/// The buffer size for receiving new messages. Defaults to 1536 bytes.
pub(crate) const DEFAULT_BUFFER_SIZE: usize = 1536;

/// The constant added to how often a message will be gossiped. Defaults to 3.
pub(crate) const DEFAULT_GOSSIP_SEND_CONSTANT: usize = 3;

/// Default number of messages to be included into a single `Gossip`.
pub(crate) const DEFAULT_GOSSIP_MAX_MESSAGES: usize = 6;

/// Builder for creating a [`SwimConfig`] with customized settings for a SWIM protocol node.
///
/// Allows configuring timeouts, intervals, and known peers in the network.
#[derive(Clone, Debug)]
pub struct SwimConfigBuilder {
    /// A list of peer node addresses
    /// that this node should initially contact.
    known_peers: Vec<String>,
    /// The duration between consecutive PING messages.
    ping_interval: Duration,
    /// The duration to wait for an ACK after sending a PING.
    ping_timeout: Duration,
    /// The number of other nodes asked to perform a PING-REQ.
    ping_req_group_size: usize,
    /// The duration to wait for an ACK after sending a PING-REQ.
    ping_req_timeout: Duration,
    /// The duration to wait for until a suspected node is decleared as deceased.
    suspect_timeout: Duration,
    /// The constant added to how often a message will be gossiped.
    gossip_send_constant: usize,
    /// How many messages should be included into a single `Gossip`.
    gossip_max_messages: usize,
}

impl SwimConfigBuilder {
    /// Creates a new [`SwimConfigBuilder`] with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Consumes the builder and returns a fully constructed [`SwimConfig`].
    pub fn build(self) -> SwimConfig {
        SwimConfig {
            known_peers: self.known_peers,
            ping_interval: self.ping_interval,
            ping_timeout: self.ping_timeout,
            ping_req_group_size: self.ping_req_group_size,
            ping_req_timeout: self.ping_req_timeout,
            suspect_timeout: self.suspect_timeout,
            gossip_send_constant: self.gossip_send_constant,
            gossip_max_messages: self.gossip_max_messages,
        }
    }

    /// Sets the known peers for this node in the cluster.
    pub fn with_known_peers<I>(mut self, known_peers: I) -> Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        self.known_peers = known_peers
            .into_iter()
            .map(|p| p.as_ref().to_string())
            .collect();
        self
    }

    /// Sets the interval between each PING message sent by this node.
    pub fn with_ping_interval(mut self, ping_interval: Duration) -> Self {
        self.ping_interval = ping_interval;
        self
    }

    /// Sets the timeout for awaiting an ACK to a PING message.
    pub fn with_ping_timeout(mut self, ping_timeout: Duration) -> Self {
        self.ping_timeout = ping_timeout;
        self
    }

    /// Sets the number of nodes involved in a PING-REQ operation when a node is suspected.
    pub fn with_ping_req_group_size(mut self, ping_req_group_size: usize) -> Self {
        self.ping_req_group_size = ping_req_group_size;
        self
    }

    /// Sets the timeout for awaiting an ACK to a PING-REQ message.
    pub fn with_ping_req_timeout(mut self, ping_req_timeout: Duration) -> Self {
        self.ping_req_timeout = ping_req_timeout;
        self
    }

    /// Sets the timeout to wait for until a suspected node is decleared as deceased.
    pub fn with_suspect_timeout(mut self, suspect_timeout: Duration) -> Self {
        self.suspect_timeout = suspect_timeout;
        self
    }

    /// Sets the constant added to how often a message will be gossiped.
    pub fn with_gossip_send_constant(mut self, gossip_send_offset: usize) -> Self {
        self.gossip_send_constant = gossip_send_offset;
        self
    }

    /// Sets the number of how many messages should be included into a single `Gossip`.
    pub fn with_gossip_max_messages(mut self, gossip_max_messages: usize) -> Self {
        self.gossip_max_messages = gossip_max_messages;
        self
    }
}

impl Default for SwimConfigBuilder {
    fn default() -> Self {
        Self {
            known_peers: vec![],
            ping_interval: DEFAULT_PING_INTERVAL,
            ping_timeout: DEFAULT_PING_TIMEOUT,
            ping_req_group_size: DEFAULT_PING_REQ_GROUP_SIZE,
            ping_req_timeout: DEFAULT_PING_REQ_TIMEOUT,
            suspect_timeout: DEFAULT_SUSPECT_TIMEOUT,
            gossip_send_constant: DEFAULT_GOSSIP_SEND_CONSTANT,
            gossip_max_messages: DEFAULT_GOSSIP_MAX_MESSAGES,
        }
    }
}

/// Configuration for a SWIM protocol node,
/// used to store parameters such as timeouts, intervals, and known peers for the node.
#[derive(Clone, Debug)]
pub struct SwimConfig {
    /// A list of peer node addresses
    /// that this node should initially contact.
    known_peers: Vec<String>,
    /// The duration between consecutive PING messages.
    ping_interval: Duration,
    /// The duration to wait for an ACK after sending a PING.
    ping_timeout: Duration,
    /// The number of other nodes asked to perform a PING-REQ.
    ping_req_group_size: usize,
    /// The duration to wait for an ACK after sending a PING-REQ.
    ping_req_timeout: Duration,
    /// The duration to wait for until a suspected node is decleared as deceased.
    suspect_timeout: Duration,
    /// The constant added to how often a message will be gossiped.
    gossip_send_constant: usize,
    /// How many messages should be included into a single `Gossip`.
    gossip_max_messages: usize,
}

impl SwimConfig {
    /// Creates a new [`SwimConfig`] with default parameters.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a new [`SwimConfigBuilder`] to construct a [`SwimConfig`].
    ///
    /// # Examples
    ///
    /// ```
    /// use swim_rs::api::config::SwimConfig;
    ///
    /// let config = SwimConfig::builder()
    ///     .with_ping_interval(std::time::Duration::from_secs(5))
    ///     .build();
    /// ```
    pub fn builder() -> SwimConfigBuilder {
        SwimConfigBuilder::new()
    }

    /// Returns a reference to the known peers of the node.
    pub fn known_peers(&self) -> &[String] {
        &self.known_peers
    }
    /// Returns the interval between PING messages.
    pub fn ping_interval(&self) -> Duration {
        self.ping_interval
    }

    /// Returns the timeout for awaiting an ACK to a PING message.
    pub fn ping_timeout(&self) -> Duration {
        self.ping_timeout
    }

    /// Returns the number of nodes involved in a PING-REQ operation.
    pub fn ping_req_group_size(&self) -> usize {
        self.ping_req_group_size
    }

    /// Returns the timeout for awaiting an ACK to a PING-REQ message.
    pub fn ping_req_timeout(&self) -> Duration {
        self.ping_req_timeout
    }

    /// Returns the duration to wait for until a suspected node is decleared as deceased.
    pub fn suspect_timeout(&self) -> Duration {
        self.suspect_timeout
    }

    /// Returns the constant added to how often a message will be gossiped.
    pub fn gossip_send_constant(&self) -> usize {
        self.gossip_send_constant
    }

    /// Returns the maximum amount of messages to be included in a `Gossip`.
    pub fn gossip_max_messages(&self) -> usize {
        self.gossip_max_messages
    }
}

impl Default for SwimConfig {
    fn default() -> Self {
        SwimConfigBuilder::new().build()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::api::config::{
        DEFAULT_GOSSIP_MAX_MESSAGES, DEFAULT_GOSSIP_SEND_CONSTANT, DEFAULT_PING_REQ_GROUP_SIZE,
        DEFAULT_PING_REQ_TIMEOUT, DEFAULT_PING_TIMEOUT, DEFAULT_SUSPECT_TIMEOUT,
    };

    use super::SwimConfig;

    #[test]
    fn test_swim_config_builder() {
        let config = SwimConfig::builder()
            .with_known_peers(&["0.0.0.0:8080"])
            .with_ping_interval(Duration::from_secs(5))
            .build();

        assert_eq!(config.known_peers(), &["0.0.0.0:8080"]);
        assert_eq!(config.ping_interval(), Duration::from_secs(5));
        assert_eq!(config.ping_timeout(), DEFAULT_PING_TIMEOUT);
        assert_eq!(config.ping_req_group_size(), DEFAULT_PING_REQ_GROUP_SIZE);
        assert_eq!(config.ping_req_timeout(), DEFAULT_PING_REQ_TIMEOUT);
        assert_eq!(config.suspect_timeout(), DEFAULT_SUSPECT_TIMEOUT);
        assert_eq!(config.gossip_send_constant(), DEFAULT_GOSSIP_SEND_CONSTANT);
        assert_eq!(config.gossip_max_messages(), DEFAULT_GOSSIP_MAX_MESSAGES);
    }
}
