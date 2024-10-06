//! # SWIM Node Module
//!
//! This module defines the `SwimNode` struct, which represents a node in the SWIM cluster.
//! The node is responsible for orchestrating the various components of the SWIM protocol,
//! including failure detection, message handling, and membership list management.
use std::sync::Arc;

use tokio::{
    sync::broadcast::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    api::config::{SwimConfig, DEFAULT_BUFFER_SIZE},
    error::Result,
    pb::{gossip::Event, Member},
    NodeState,
};

use super::{
    detection::{FailureDetector, FailureDetectorState},
    disseminate::Disseminator,
    member::MembershipList,
    message::MessageHandler,
    transport::TransportLayer,
};

/// A macro to await a future and log any errors that occur.
///
/// This macro simplifies error handling in asynchronous contexts by logging
/// errors with a custom message.
macro_rules! await_and_log_error {
    ($expr:expr, $name:expr) => {{
        if let Err(e) = $expr.await {
            tracing::error!("{}: {}", $name, e.to_string());
        }
    }};
}

/// Represents a node in the SWIM cluster.
///
/// The `SwimNode` struct encapsulates the components necessary for running the SWIM protocol,
/// including the failure detector, message handler, and membership list. It provides methods
/// to initialize the node, run the protocol, and interact with the node's state.
#[derive(Clone, Debug)]
pub(crate) struct SwimNode<T: TransportLayer> {
    /// The address of this node.
    addr: String,
    /// The SWIM configuration settings.
    config: Arc<SwimConfig>,
    /// The failure detector responsible for monitoring node health.
    failure_detector: Arc<FailureDetector<T>>,
    /// The message handler for processing incoming SWIM messages.
    message_handler: Arc<MessageHandler<T>>,
    /// The membership list containing information about cluster members.
    membership_list: Arc<MembershipList>,
    /// The broadcast channel for emitting events.
    tx: Sender<Event>,
}

impl<T: TransportLayer + Send + Sync + 'static> SwimNode<T> {
    /// Creates a new `SwimNode` instance with the given socket, configuration, and event sender.
    ///
    /// This method initializes the membership list with the known peers from the configuration
    /// and sets up the necessary components for the SWIM protocol.
    ///
    /// # Arguments
    ///
    /// * `socket` - The transport layer socket for communication.
    /// * `config` - The SWIM configuration settings.
    /// * `tx` - The sender part of a broadcast channel for emitting events.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the new `SwimNode` instance or an error if initialization fails.
    pub(crate) async fn try_new(socket: T, config: SwimConfig, tx: Sender<Event>) -> Result<Self> {
        let addr = socket.local_addr()?;
        let membership_list = MembershipList::new(&addr, 0);
        let iter = config
            .known_peers()
            .iter()
            .map(|addr| Member::new(addr, NodeState::Alive, 0));
        membership_list.update_from_iter(iter).await;

        Self::try_new_with_membership_list(socket, config, membership_list, tx)
    }

    /// Creates a new `SwimNode` instance with an existing membership list.
    ///
    /// This method allows for providing a pre-initialized membership list, which can be useful
    /// for testing or when the membership list is constructed separately.
    pub(crate) fn try_new_with_membership_list(
        socket: T,
        config: SwimConfig,
        membership_list: MembershipList,
        tx: Sender<Event>,
    ) -> Result<Self> {
        let addr = socket.local_addr()?;
        let config = Arc::new(config);
        let socket = Arc::new(socket);
        let membership_list = Arc::new(membership_list);

        let disseminator = Arc::new(Disseminator::new(
            config.gossip_max_messages(),
            DEFAULT_BUFFER_SIZE,
            config.gossip_send_constant(),
        ));
        let failure_detector = Arc::new(FailureDetector::new(
            &addr,
            socket.clone(),
            config.clone(),
            membership_list.clone(),
            disseminator.clone(),
            tx.clone(),
        ));
        let message_handler = Arc::new(MessageHandler::new(
            &addr,
            socket.clone(),
            membership_list.clone(),
            disseminator.clone(),
            tx.clone(),
        ));

        Ok(Self {
            addr,
            config,
            failure_detector,
            message_handler,
            membership_list,
            tx,
        })
    }

    /// Returns the address of this node.
    pub(crate) fn addr(&self) -> &str {
        &self.addr
    }

    /// Returns a reference to the SWIM configuration.
    pub(crate) fn config(&self) -> &SwimConfig {
        &self.config
    }

    /// Returns a reference to the membership list.
    pub(crate) fn membership_list(&self) -> &MembershipList {
        &self.membership_list
    }

    /// Subscribes to the event broadcast channel.
    pub(crate) fn subscribe(&self) -> Receiver<Event> {
        self.tx.subscribe()
    }

    /// Runs the SWIM protocol by starting the failure detection and message dispatch loops.
    pub(crate) async fn run(&self) -> (JoinHandle<()>, JoinHandle<()>) {
        let dispatch_handle = self.dispatch().await;
        let detection_handle = self.failure_detection().await;

        (dispatch_handle, detection_handle)
    }

    /// Starts the failure detection loop.
    ///
    /// This method spawns a task that continuously performs failure detection
    /// based on the current state of the failure detector.
    async fn failure_detection(&self) -> JoinHandle<()> {
        let failure_detector = self.failure_detector.clone();
        let membership_list = self.membership_list.clone();

        tokio::spawn(async move {
            loop {
                membership_list.wait_for_members().await;

                let state = failure_detector.state().await;
                match state {
                    FailureDetectorState::SendingPing => {
                        await_and_log_error!(failure_detector.send_ping(), "SendPingError");
                    }
                    FailureDetectorState::SendingPingReq {
                        target,
                        incarnation,
                    } => {
                        await_and_log_error!(
                            failure_detector.send_ping_req(&target, incarnation),
                            "SendPingReqError"
                        );
                    }
                    FailureDetectorState::WaitingForAck {
                        target,
                        ack_type,
                        incarnation,
                    } => {
                        await_and_log_error!(
                            failure_detector.wait_for_ack(&target, &ack_type, incarnation),
                            "WaitingForAckError"
                        );
                    }
                    FailureDetectorState::DeclaringNodeAsDead {
                        target,
                        incarnation,
                    } => {
                        await_and_log_error!(
                            failure_detector.declare_node_as_dead(&target, incarnation),
                            "DeclareNodeAsDeadError"
                        );
                    }
                }
            }
        })
    }

    /// Starts the message dispatch loop.
    ///
    /// This method spawns a task that continuously reads incoming messages
    /// and dispatches them to the appropriate handlers.
    async fn dispatch(&self) -> JoinHandle<()> {
        let message_handler = self.message_handler.clone();

        tokio::spawn(async move {
            loop {
                await_and_log_error!(message_handler.dispatch_action(), "DispatchError");
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::broadcast;

    use crate::{api::config::SwimConfig, core::node::SwimNode, test_utils::mocks::MockUdpSocket};

    #[tokio::test]
    async fn test_node_init_with_known_peers() {
        let socket = MockUdpSocket::new();
        let config = SwimConfig::builder().with_known_peers(&["NODE_B"]).build();
        let (tx, _) = broadcast::channel(32);

        let node = SwimNode::try_new(socket, config, tx).await.unwrap();
        assert_eq!(node.membership_list().len(), 2);
    }
}
