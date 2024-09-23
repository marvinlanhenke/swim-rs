use std::time::Duration;

/// Default interval between each PING message sent by a node.
const DEFAULT_PING_INTERVAL: Duration = Duration::from_millis(3000);

/// Default timeout for a PING message.
/// If no ACK is received within this duration, the node is considered a `Suspect`.
const DEFAULT_PING_TIMEOUT: Duration = Duration::from_millis(1000);

/// Default number of nodes included in a PING-REQ request.
const DEFAULT_PING_REQ_GROUP_SIZE: usize = 1;

/// Default timeout for a PING-REQ message.
/// If no ACK is received within this time, the suspected node is still considered unreachable.
const DEFAULT_PING_REQ_TIMEOUT: Duration = Duration::from_millis(1000);

/// The buffer size for receiving new messages. Defaults to 1536 bytes.
pub(crate) const DEFAULT_BUFFER_SIZE: usize = 1536;

/// Builder for creating a [`SwimConfig`] with customized settings for a SWIM protocol node.
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
        }
    }

    /// Sets the known peers for this node in the cluster.
    pub fn with_known_peers<T>(mut self, known_peers: T) -> Self
    where
        T: IntoIterator,
        T::Item: AsRef<str>,
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
}

impl Default for SwimConfigBuilder {
    fn default() -> Self {
        Self {
            known_peers: vec![],
            ping_interval: DEFAULT_PING_INTERVAL,
            ping_timeout: DEFAULT_PING_TIMEOUT,
            ping_req_group_size: DEFAULT_PING_REQ_GROUP_SIZE,
            ping_req_timeout: DEFAULT_PING_REQ_TIMEOUT,
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
}

impl SwimConfig {
    /// Creates a new [`SwimConfig`] with default parameters.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a new [`SwimConfigBuilder`] to construct a [`SwimConfig`].
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
}

impl Default for SwimConfig {
    fn default() -> Self {
        SwimConfigBuilder::new().build()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::config::{
        DEFAULT_PING_REQ_GROUP_SIZE, DEFAULT_PING_REQ_TIMEOUT, DEFAULT_PING_TIMEOUT,
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
    }
}
