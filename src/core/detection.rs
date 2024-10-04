//! # Failure Detector Module
//!
//! This module implements the failure detection mechanism for the SWIM protocol.
//! It handles sending and receiving ping messages, detecting failed nodes,
//! and managing the state transitions of nodes in the cluster.
use std::sync::Arc;

use tokio::sync::broadcast::Sender;
use tokio::sync::RwLock;

use crate::api::config::SwimConfig;
use crate::core::utils::send_action;
use crate::error::Result;
use crate::pb::swim_message::Action;
use crate::pb::{Member, NodeState};
use crate::{emit_and_disseminate_event, Event};

use super::disseminate::{Disseminator, DisseminatorUpdate};
use super::member::MembershipList;
use super::transport::TransportLayer;

/// Represents the type of acknowledgment expected or received.
///
/// Used to distinguish between acknowledgments for direct ping messages
/// and indirect ping requests (`ping-req`)
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum AckType {
    /// Acknowledgment for a direct ping message.
    PingAck,
    /// Acknowledgment for an indirect ping request (`ping-req`).
    PingReqAck,
}

/// Represents the various states of the failure detector during its operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FailureDetectorState {
    SendingPing,
    SendingPingReq {
        target: String,
        incarnation: u64,
    },
    WaitingForAck {
        target: String,
        ack_type: AckType,
        incarnation: u64,
    },
    DeclaringNodeAsDead {
        target: String,
        incarnation: u64,
    },
}

/// The `FailureDetector` is responsible for monitoring the health of nodes in the cluster.
///
/// It periodically sends ping messages to other nodes, handles acknowledgments,
/// and initiates the suspicion mechanism when nodes are unresponsive.
/// It updates the membership list and disseminates events accordingly.
#[derive(Clone, Debug)]
pub(crate) struct FailureDetector<T: TransportLayer> {
    /// The address of this node.
    addr: String,
    /// The [`TransportLayer`] used for sending and receiving messages.
    socket: Arc<T>,
    /// The current state of the failure detector.
    state: Arc<RwLock<FailureDetectorState>>,
    /// Configuration parameters for the SWIM protocol.
    config: Arc<SwimConfig>,
    /// The membership list of nodes in the cluster.
    membership_list: Arc<MembershipList>,
    /// The disseminator for broadcasting events to other nodes.
    disseminator: Arc<Disseminator>,
    /// Channel for sending events to subscribers.
    tx: Sender<Event>,
}

impl<T: TransportLayer> FailureDetector<T> {
    /// Creates a new instance of the `FailureDetector`.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address of this node.
    /// * `socket` - The transport layer used for communication.
    /// * `config` - Configuration settings for the SWIM protocol.
    /// * `membership_list` - The shared membership list.
    /// * `disseminator` - The disseminator for propagating events.
    /// * `tx` - The sender part of a broadcast channel for emitting events.
    ///
    /// # Returns
    ///
    /// A new `FailureDetector` instance.
    pub(crate) fn new(
        addr: impl Into<String>,
        socket: Arc<T>,
        config: Arc<SwimConfig>,
        membership_list: Arc<MembershipList>,
        disseminator: Arc<Disseminator>,
        tx: Sender<Event>,
    ) -> Self {
        let addr = addr.into();
        let state = Arc::new(RwLock::new(FailureDetectorState::SendingPing));

        Self {
            addr,
            socket,
            state,
            config,
            membership_list,
            disseminator,
            tx,
        }
    }

    /// Retrieves the current state of the failure detector.
    pub(crate) async fn state(&self) -> FailureDetectorState {
        let state = self.state.read().await;
        (*state).clone()
    }

    /// Sends a ping message to a target node to check its liveness.
    ///
    /// This method selects a target node from the membership list, sends a ping,
    /// and updates the state to wait for an acknowledgment.
    pub(crate) async fn send_ping(&self) -> Result<()> {
        tokio::time::sleep(self.config.ping_interval()).await;

        let gossip = self
            .disseminator
            .get_gossip(self.membership_list.len())
            .await;

        let action = Action::new_ping(&self.addr, "", gossip);

        if let Some(member) = self.membership_list.get_member_list(1, None).await.first() {
            let target = member.addr.clone();

            tracing::debug!("[{}] sending PING to {}", &self.addr, &target);
            send_action(&*self.socket, &action, &target).await?;

            self.membership_list.update_member(Member::new(
                &target,
                NodeState::Pending,
                member.incarnation,
            ));

            let mut state = self.state.write().await;
            *state = FailureDetectorState::WaitingForAck {
                target,
                ack_type: AckType::PingAck,
                incarnation: member.incarnation,
            };
        };

        Ok(())
    }

    /// Sends a ping request (`ping-req`) to other nodes to indirectly check the liveness of a suspect node.
    ///
    /// If a node does not respond to a direct ping, this method sends `ping-req` messages
    /// to a group of other nodes, asking them to ping the suspect node on behalf of this node.
    pub(crate) async fn send_ping_req(
        &self,
        suspect: impl Into<String>,
        incarnation: u64,
    ) -> Result<()> {
        let suspect = suspect.into();

        self.membership_list.update_member(Member::new(
            &suspect,
            NodeState::Suspected,
            incarnation,
        ));

        emit_and_disseminate_event!(
            &self,
            Event::new_node_suspected(&self.addr, &suspect, incarnation),
            DisseminatorUpdate::NodesAlive
        );

        let probe_group = self
            .membership_list
            .get_member_list(self.config.ping_req_group_size(), Some(&suspect))
            .await;

        for probe_member in &probe_group {
            tracing::debug!(
                "[{}] sending PING_REQ to {}",
                &self.addr,
                &probe_member.addr
            );

            let gossip = self
                .disseminator
                .get_gossip(self.membership_list.len())
                .await;
            let action = Action::new_ping_req(&self.addr, &suspect, gossip);

            send_action(&*self.socket, &action, &probe_member.addr).await?;
        }

        let mut state = self.state.write().await;
        *state = FailureDetectorState::WaitingForAck {
            target: suspect,
            ack_type: AckType::PingReqAck,
            incarnation,
        };

        Ok(())
    }

    /// Waits for an acknowledgment from a target node within a specified timeout.
    ///
    /// Depending on the `AckType`, it waits for a direct ping acknowledgment or
    /// an acknowledgment from a ping request (`ping-req`).
    pub(crate) async fn wait_for_ack(
        &self,
        target: impl Into<String>,
        ack_type: &AckType,
        incarnation: u64,
    ) -> Result<()> {
        let target = target.into();

        match ack_type {
            AckType::PingAck => {
                tracing::debug!("[{}] waiting for PING_ACK from {}", &self.addr, &target);
                tokio::time::sleep(self.config.ping_timeout()).await;

                let mut state = self.state.write().await;

                match self.membership_list.member_state(&target) {
                    Some(node_state) => match node_state? {
                        NodeState::Alive => *state = FailureDetectorState::SendingPing,
                        _ => {
                            *state = FailureDetectorState::SendingPingReq {
                                target,
                                incarnation,
                            }
                        }
                    },
                    None => *state = FailureDetectorState::SendingPing,
                };

                Ok(())
            }
            AckType::PingReqAck => {
                tracing::debug!("[{}] waiting for PING_REQ_ACK from {}", &self.addr, &target);
                tokio::time::sleep(self.config.ping_req_timeout()).await;

                let mut state = self.state.write().await;

                match self.membership_list.member_state(&target) {
                    Some(node_state) => match node_state? {
                        NodeState::Alive => *state = FailureDetectorState::SendingPing,
                        _ => {
                            *state = FailureDetectorState::DeclaringNodeAsDead {
                                target,
                                incarnation,
                            }
                        }
                    },
                    None => *state = FailureDetectorState::SendingPing,
                };

                Ok(())
            }
        }
    }

    /// Declares a suspect node as dead after it fails to respond within the `suspect timeout` period.
    ///
    /// This method updates the membership list, disseminates the event to other nodes,
    /// and notifies subscribers about the node's death.
    pub(crate) async fn declare_node_as_dead(
        &self,
        target: impl Into<String>,
        incarnation: u64,
    ) -> Result<()> {
        let addr = self.addr.clone();
        let target = target.into();
        let membership_list = self.membership_list.clone();
        let suspect_timeout = self.config.suspect_timeout();
        let disseminator = self.disseminator.clone();
        let tx = self.tx.clone();

        tokio::spawn(async move {
            tracing::debug!("[{}] declaring NODE {} as deceased", &addr, &target);
            tokio::time::sleep(suspect_timeout).await;

            membership_list.remove_member(&target).await;

            let event = Event::new_node_deceased(&addr, &target, incarnation);

            disseminator
                .push(DisseminatorUpdate::NodesDeceased(event.clone()))
                .await;

            if let Err(e) = tx.send(event) {
                tracing::debug!("SendEventError: {}", e.to_string());
            };
        });

        let mut state = self.state.write().await;
        *state = FailureDetectorState::SendingPing;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::sync::broadcast;

    use crate::{
        api::config::{SwimConfig, DEFAULT_BUFFER_SIZE, DEFAULT_GOSSIP_MAX_MESSAGES},
        core::{
            detection::{AckType, FailureDetectorState},
            disseminate::Disseminator,
            member::MembershipList,
        },
        pb::{swim_message::Action, Member, NodeState, SwimMessage},
        test_utils::mocks::MockUdpSocket,
    };

    use super::FailureDetector;

    async fn create_failure_detector() -> FailureDetector<MockUdpSocket> {
        let socket = Arc::new(MockUdpSocket::new());
        let config = Arc::new(
            SwimConfig::builder()
                .with_ping_interval(Duration::from_millis(0))
                .with_ping_timeout(Duration::from_millis(0))
                .with_ping_req_timeout(Duration::from_millis(0))
                .build(),
        );
        let membership_list = Arc::new(MembershipList::new("NODE_A", 0));
        membership_list.add_member("NODE_B", 0).await;

        let (tx, _) = broadcast::channel(32);
        let disseminator = Arc::new(Disseminator::new(
            DEFAULT_GOSSIP_MAX_MESSAGES,
            DEFAULT_BUFFER_SIZE,
            1,
        ));

        FailureDetector::new("NODE_A", socket, config, membership_list, disseminator, tx)
    }

    #[tokio::test]
    async fn test_detection_declare_node_as_dead() {
        let failure_detector = create_failure_detector().await;
        failure_detector
            .declare_node_as_dead("NODE_B", 0)
            .await
            .unwrap();

        let result = failure_detector.state().await;
        let expected = FailureDetectorState::SendingPing;
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_detection_wait_for_ack_ping_req() {
        let failure_detector = create_failure_detector().await;
        failure_detector.membership_list.update_member(Member::new(
            "NODE_B",
            NodeState::Suspected,
            0,
        ));

        failure_detector
            .wait_for_ack("NODE_B", &AckType::PingReqAck, 0)
            .await
            .unwrap();

        let result = failure_detector.state().await;
        let expected = FailureDetectorState::DeclaringNodeAsDead {
            target: "NODE_B".to_string(),
            incarnation: 0,
        };
        assert_eq!(result, expected);

        failure_detector
            .membership_list
            .update_member(Member::new("NODE_B", NodeState::Alive, 1));

        failure_detector
            .wait_for_ack("NODE_B", &AckType::PingReqAck, 1)
            .await
            .unwrap();

        let result = failure_detector.state().await;
        let expected = FailureDetectorState::SendingPing;
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_detection_wait_for_ack_ping() {
        let failure_detector = create_failure_detector().await;
        failure_detector.membership_list.update_member(Member::new(
            "NODE_B",
            NodeState::Pending,
            0,
        ));

        failure_detector
            .wait_for_ack("NODE_B", &AckType::PingAck, 0)
            .await
            .unwrap();

        let result = failure_detector.state().await;
        let expected = FailureDetectorState::SendingPingReq {
            target: "NODE_B".to_string(),
            incarnation: 0,
        };
        assert_eq!(result, expected);

        failure_detector
            .membership_list
            .update_member(Member::new("NODE_B", NodeState::Alive, 1));

        failure_detector
            .wait_for_ack("NODE_B", &AckType::PingAck, 1)
            .await
            .unwrap();

        let result = failure_detector.state().await;
        let expected = FailureDetectorState::SendingPing;
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_detection_send_ping_req() {
        let failure_detector = create_failure_detector().await;
        failure_detector
            .membership_list
            .add_member("NODE_C", 0)
            .await;

        failure_detector.send_ping_req("NODE_B", 0).await.unwrap();

        let result = failure_detector
            .membership_list
            .member_state("NODE_B")
            .unwrap()
            .unwrap();
        let expected = NodeState::Suspected;
        assert_eq!(result, expected);

        let result = failure_detector.state().await;
        let expected = FailureDetectorState::WaitingForAck {
            target: "NODE_B".to_string(),
            ack_type: AckType::PingReqAck,
            incarnation: 0,
        };
        assert_eq!(result, expected);

        let result = matches!(
            &failure_detector.socket.transmitted().await[0],
            SwimMessage {
                action: Some(Action::PingReq(_))
            }
        );
        assert!(result);
    }

    #[tokio::test]
    async fn test_detection_send_ping() {
        let failure_detector = create_failure_detector().await;
        let gossip = failure_detector
            .disseminator
            .get_gossip(failure_detector.membership_list.len())
            .await;

        failure_detector.send_ping().await.unwrap();

        let result = failure_detector
            .membership_list
            .member_state("NODE_B")
            .unwrap()
            .unwrap();
        let expected = NodeState::Pending;
        assert_eq!(result, expected);

        let result = failure_detector.state().await;
        let expected = FailureDetectorState::WaitingForAck {
            target: "NODE_B".to_string(),
            ack_type: AckType::PingAck,
            incarnation: 0,
        };
        assert_eq!(result, expected);

        let result = &failure_detector.socket.transmitted().await[0];
        let expected = SwimMessage {
            action: Some(Action::new_ping("NODE_A", "", gossip)),
        };
        assert_eq!(result, &expected);
    }
}
