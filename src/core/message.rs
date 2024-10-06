//! # Message Handler Module
//!
//! This module implements the `MessageHandler` struct and associated methods,
//! which are responsible for handling incoming SWIM protocol messages.
//! The `MessageHandler` processes different types of messages (e.g., PING, PING_REQ, ACK),
//! handles gossip dissemination, and updates the membership list accordingly.
use std::sync::Arc;

use crate::{
    api::config::DEFAULT_BUFFER_SIZE,
    core::{disseminate::DisseminatorUpdate, utils::send_action},
    emit_and_disseminate_event,
    error::{Error, Result},
    pb::{
        gossip::{NodeJoined, NodeRecovered, NodeSuspected},
        swim_message::{self, Ack, Action, Ping, PingReq},
        Gossip, Member, NodeState, SwimMessage,
    },
    Event, NodeDeceased,
};

use prost::Message;
use snafu::location;
use tokio::sync::broadcast::Sender;

use super::{disseminate::Disseminator, member::MembershipList, transport::TransportLayer};

/// The `MessageHandler` is responsible for handling incoming SWIM protocol messages.
///
/// It processes different actions such as PING, PING_REQ, and ACK,
/// handles gossip messages, updates the membership list, and disseminates events.
#[derive(Clone, Debug)]
pub(crate) struct MessageHandler<T: TransportLayer> {
    /// The address of this node.
    addr: String,
    /// The transport layer used for sending and receiving messages.
    socket: Arc<T>,
    /// The membership list of nodes in the cluster.
    membership_list: Arc<MembershipList>,
    /// The disseminator for propagating gossip messages.
    disseminator: Arc<Disseminator>,
    /// Channel for sending events to subscribers.
    tx: Sender<Event>,
}

impl<T: TransportLayer> MessageHandler<T> {
    /// Creates a new instance of the `MessageHandler`.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address of this node.
    /// * `socket` - The transport layer used for communication.
    /// * `membership_list` - The shared membership list.
    /// * `disseminator` - The disseminator for propagating gossip messages.
    /// * `tx` - The sender part of a broadcast channel for emitting events.
    ///
    /// # Returns
    ///
    /// A new `MessageHandler` instance.
    pub(crate) fn new(
        addr: impl Into<String>,
        socket: Arc<T>,
        membership_list: Arc<MembershipList>,
        disseminator: Arc<Disseminator>,
        tx: Sender<Event>,
    ) -> Self {
        let addr = addr.into();

        Self {
            addr,
            socket,
            membership_list,
            disseminator,
            tx,
        }
    }

    /// Dispatches incoming actions by reading from the socket and handling the message accordingly.
    ///
    /// This method reads a message from the socket, decodes it, and dispatches it to the appropriate handler
    /// based on the action type (PING, PING_REQ, ACK).
    pub(crate) async fn dispatch_action(&self) -> Result<()> {
        use swim_message::Action::*;

        let mut buf = [0u8; DEFAULT_BUFFER_SIZE];
        let len = self.socket.recv(&mut buf).await?;
        let message = SwimMessage::decode(&buf[..len])?;

        match message.action {
            Some(action) => match action {
                Ping(v) => self.handle_ping(&v).await,
                PingReq(v) => self.handle_ping_req(&v).await,
                Ack(v) => self.handle_ack(&v).await,
            },
            None => Err(Error::InvalidData {
                message: "Message must contain an 'action'".to_string(),
                location: location!(),
            }),
        }
    }

    /// Handles an incoming PING message.
    ///
    /// Processes any gossip messages included with the PING, updates the membership list,
    /// and sends an ACK back to the sender or the original requester if it's a forwarded PING.
    pub(crate) async fn handle_ping(&self, action: &Ping) -> Result<()> {
        tracing::debug!("[{}] handling {action:#?}", &self.addr);

        self.handle_gossip(&action.gossip).await;

        // If the node is unknown, we check if it is currently gossiped as deceased,
        // and increase the incarnation number accordingly. This allows us to identify
        // the `NodeJoined` event as more recently.
        if !self.membership_list.members().contains_key(&action.from) {
            let incarnation = self
                .disseminator
                .is_deceased(&action.from)
                .await
                .map(|n| n + 1)
                .unwrap_or(0);
            self.membership_list
                .add_member(&action.from, incarnation)
                .await;
            emit_and_disseminate_event!(
                &self,
                Event::new_node_joined(&self.addr, &action.from, incarnation),
                DisseminatorUpdate::NodesAlive
            );
        }

        let gossip = self
            .disseminator
            .get_gossip(self.membership_list.len())
            .await;
        let message = Action::new_ack(&self.addr, &action.requested_by, gossip);

        // We check if the PING was `requested_by` a PING_REQ,
        // if it was we send the ACK to the original issuer.
        let target = match action.requested_by.is_empty() {
            true => &action.from,
            false => &action.requested_by,
        };

        send_action(&*self.socket, &message, target).await
    }

    /// Handles an incoming PING_REQ message.
    ///
    /// Processes any gossip messages included with the PING_REQ, and sends a PING to the suspect node
    /// on behalf of the original requester.
    pub(crate) async fn handle_ping_req(&self, action: &PingReq) -> Result<()> {
        tracing::debug!("[{}] handling {action:?}", &self.addr);

        self.handle_gossip(&action.gossip).await;

        let gossip = self
            .disseminator
            .get_gossip(self.membership_list.len())
            .await;

        let message = Action::new_ping(&self.addr, &action.from, gossip);

        send_action(&*self.socket, &message, &action.suspect).await
    }

    /// Handles an incoming ACK message.
    ///
    /// Processes any gossip messages included with the ACK, updates the membership list,
    /// and forwards the ACK if necessary.
    pub(crate) async fn handle_ack(&self, action: &Ack) -> Result<()> {
        tracing::debug!("[{}] handling {action:?}", &self.addr);

        self.handle_gossip(&action.gossip).await;

        match action.forward_to.is_empty() {
            true => {
                if let Some(incarnation) = self.membership_list.member_incarnation(&action.from) {
                    self.membership_list.update_member(Member::new(
                        &action.from,
                        NodeState::Alive,
                        incarnation,
                    ))
                }
            }
            false => {
                let message = Action::new_ack(&action.from, "", action.gossip.clone());
                send_action(&*self.socket, &message, &action.forward_to).await?
            }
        };

        Ok(())
    }

    /// Handles gossip messages included in actions.
    ///
    /// Processes each gossip message and calls the appropriate handler based on the event type.
    async fn handle_gossip(&self, gossip: &[Gossip]) {
        for message in gossip {
            if let Some(event) = &message.event {
                tracing::debug!("[{}] handling gossip {:#?}", &self.addr, event);
                match event {
                    Event::NodeJoined(evt) => self.handle_node_joined(evt).await,
                    Event::NodeRecovered(evt) => self.handle_node_recovered(evt).await,
                    Event::NodeSuspected(evt) => self.handle_node_suspected(evt).await,
                    Event::NodeDeceased(evt) => self.handle_node_deceased(evt).await,
                }
            }
        }
    }

    /// Handles a `NodeJoined` gossip event.
    ///
    /// Updates the membership list with the new member and disseminates the event if necessary.
    async fn handle_node_joined(&self, event: &NodeJoined) {
        let incoming_incarnation = self
            .disseminator
            .is_deceased(&event.new_member)
            .await
            .map(|n| n + 1)
            .unwrap_or(0)
            .max(event.joined_incarnation_no);

        let opt_member = self
            .membership_list
            .members()
            .get(&event.new_member)
            .map(|m| m.value().clone());

        let should_emit = match opt_member {
            Some(member) => match incoming_incarnation > member.incarnation {
                true => {
                    self.membership_list.update_member(Member {
                        addr: member.addr.clone(),
                        state: member.state,
                        incarnation: incoming_incarnation,
                    });
                    true
                }
                false => false,
            },
            None => {
                self.membership_list
                    .add_member(&event.new_member, incoming_incarnation)
                    .await
            }
        };

        if should_emit {
            emit_and_disseminate_event!(
                &self,
                Event::new_node_joined(&self.addr, &event.new_member, incoming_incarnation),
                DisseminatorUpdate::NodesAlive
            );
        }
    }

    /// Handles a `NodeRecovered` gossip event.
    ///
    /// Updates the membership list to mark the node as alive and disseminates the event if necessary.
    async fn handle_node_recovered(&self, event: &NodeRecovered) {
        let incoming_incarnation = event.recovered_incarnation_no;

        if let Some(current_incarnation) = self.membership_list.member_incarnation(&event.recovered)
        {
            if incoming_incarnation > current_incarnation {
                self.membership_list.update_member(Member::new(
                    &event.recovered,
                    NodeState::Alive,
                    incoming_incarnation,
                ));

                emit_and_disseminate_event!(
                    &self,
                    Event::new_node_recovered(&self.addr, &event.recovered, incoming_incarnation),
                    DisseminatorUpdate::NodesAlive
                );
            }
        }
    }

    /// Handles a `NodeSuspected` gossip event.
    ///
    /// Updates the membership list to mark the node as suspected and disseminates the event if necessary.
    /// If the current node itself is suspected, it initiates a recovery process.
    async fn handle_node_suspected(&self, event: &NodeSuspected) {
        let is_suspected = event.suspect == self.addr;

        match is_suspected {
            true => {
                let incarnation = event.suspect_incarnation_no + 1;

                emit_and_disseminate_event!(
                    &self,
                    Event::new_node_recovered(&self.addr, &self.addr, incarnation),
                    DisseminatorUpdate::NodesAlive
                );
            }
            false => {
                let incoming_incarnation = event.suspect_incarnation_no;

                if let Some(current_incarnation) =
                    self.membership_list.member_incarnation(&event.suspect)
                {
                    let was_alive = matches!(
                        self.membership_list.member_state(&event.suspect),
                        Some(Ok(NodeState::Alive))
                    );
                    let should_update = (was_alive && incoming_incarnation >= current_incarnation)
                        || (!was_alive && incoming_incarnation > current_incarnation);

                    if should_update {
                        self.membership_list.update_member(Member::new(
                            &event.suspect,
                            NodeState::Suspected,
                            incoming_incarnation,
                        ));

                        emit_and_disseminate_event!(
                            &self,
                            Event::new_node_suspected(
                                &self.addr,
                                &event.suspect,
                                incoming_incarnation
                            ),
                            DisseminatorUpdate::NodesAlive
                        );
                    }
                }
            }
        }
    }

    /// Handles a `NodeDeceased` gossip event.
    ///
    /// Removes the deceased node from the membership list and disseminates the event if necessary.
    async fn handle_node_deceased(&self, event: &NodeDeceased) {
        let deceased = &event.deceased;
        let incoming_incarnation = event.deceased_incarnation_no;

        if let Some(current_incarnation) = self.membership_list.member_incarnation(deceased) {
            if incoming_incarnation >= current_incarnation {
                self.membership_list.remove_member(&event.deceased).await;

                emit_and_disseminate_event!(
                    &self,
                    Event::new_node_deceased(
                        &self.addr,
                        &event.deceased,
                        event.deceased_incarnation_no
                    ),
                    DisseminatorUpdate::NodesDeceased
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::broadcast;

    use crate::{
        api::config::{DEFAULT_BUFFER_SIZE, DEFAULT_GOSSIP_MAX_MESSAGES},
        core::{
            disseminate::{Disseminator, DisseminatorUpdate},
            member::MembershipList,
            message::MessageHandler,
        },
        pb::{
            swim_message::{Ack, Action, Ping, PingReq},
            Gossip, Member, NodeState, SwimMessage,
        },
        test_utils::mocks::MockUdpSocket,
        Event,
    };

    async fn create_message_handler() -> MessageHandler<MockUdpSocket> {
        let socket = Arc::new(MockUdpSocket::new());

        let membership_list = Arc::new(MembershipList::new("NODE_A", 0));
        membership_list.add_member("NODE_B", 0).await;

        let (tx, _) = broadcast::channel(32);

        let disseminator = Arc::new(Disseminator::new(
            DEFAULT_GOSSIP_MAX_MESSAGES,
            DEFAULT_BUFFER_SIZE,
            1,
        ));

        MessageHandler::new("NODE_A", socket, membership_list, disseminator, tx)
    }

    #[tokio::test]
    async fn test_message_handle_ping_with_node_deceased() {
        let message_handler = create_message_handler().await;

        let gossip = Gossip {
            event: Some(Event::new_node_deceased("NODE_B", "NODE_A", 0)),
        };

        let action = Ping {
            from: "NODE_B".to_string(),
            requested_by: "".to_string(),
            gossip: vec![gossip],
        };

        message_handler.handle_ping(&action).await.unwrap();

        assert_eq!(message_handler.membership_list.len(), 1);
        assert!(!message_handler
            .membership_list
            .members()
            .contains_key("NODE_A"));
    }

    #[tokio::test]
    async fn test_message_handle_ping_with_node_suspected() {
        let message_handler = create_message_handler().await;

        let gossip = Gossip {
            event: Some(Event::new_node_suspected("NODE_A", "NODE_B", 0)),
        };

        let action = Ping {
            from: "NODE_A".to_string(),
            requested_by: "".to_string(),
            gossip: vec![gossip],
        };

        message_handler.handle_ping(&action).await.unwrap();

        let result = message_handler
            .membership_list
            .member_state("NODE_B")
            .unwrap()
            .unwrap();
        assert_eq!(result, NodeState::Suspected);
    }

    #[tokio::test]
    async fn test_message_handle_ping_with_node_recovered_lower_incarnation_id() {
        let message_handler = create_message_handler().await;
        message_handler.membership_list.update_member(Member::new(
            "NODE_B",
            NodeState::Suspected,
            99,
        ));

        let gossip = Gossip {
            event: Some(Event::new_node_recovered("NODE_B", "NODE_B", 1)),
        };

        let action = Ping {
            from: "NODE_B".to_string(),
            requested_by: "".to_string(),
            gossip: vec![gossip],
        };

        message_handler.handle_ping(&action).await.unwrap();

        let expected = message_handler
            .membership_list
            .member_state("NODE_B")
            .unwrap()
            .unwrap();
        assert_eq!(NodeState::Suspected, expected);
    }

    #[tokio::test]
    async fn test_message_handle_ping_with_node_recovered() {
        let message_handler = create_message_handler().await;
        message_handler.membership_list.update_member(Member::new(
            "NODE_B",
            NodeState::Suspected,
            0,
        ));

        let gossip = Gossip {
            event: Some(Event::new_node_recovered("NODE_B", "NODE_B", 1)),
        };

        let action = Ping {
            from: "NODE_B".to_string(),
            requested_by: "".to_string(),
            gossip: vec![gossip],
        };

        message_handler.handle_ping(&action).await.unwrap();

        let expected = message_handler
            .membership_list
            .member_state("NODE_B")
            .unwrap()
            .unwrap();
        assert_eq!(NodeState::Alive, expected);
    }

    #[tokio::test]
    async fn test_message_handle_ping_with_node_joined_unknown_with_lt_incarnation_id_deceased() {
        let message_handler = create_message_handler().await;
        message_handler
            .disseminator
            .push(DisseminatorUpdate::NodesDeceased(Event::new_node_deceased(
                "NODE_A", "NODE_C", 0,
            )))
            .await;
        let gossip = vec![Gossip {
            event: Some(Event::new_node_joined("NODE_A", "NODE_C", 0)),
        }];

        let action = Ping {
            from: "NODE_B".to_string(),
            requested_by: "".to_string(),
            gossip,
        };

        message_handler.handle_ping(&action).await.unwrap();

        assert_eq!(message_handler.membership_list.len(), 3);
        assert_eq!(
            message_handler.membership_list.member_incarnation("NODE_C"),
            Some(1)
        );
    }

    #[tokio::test]
    async fn test_message_handle_ping_with_node_joined_unknown_with_gt_incarnation_id_deceased() {
        let message_handler = create_message_handler().await;
        message_handler
            .disseminator
            .push(DisseminatorUpdate::NodesDeceased(Event::new_node_deceased(
                "NODE_A", "NODE_C", 0,
            )))
            .await;
        let gossip = vec![Gossip {
            event: Some(Event::new_node_joined("NODE_A", "NODE_C", 2)),
        }];

        let action = Ping {
            from: "NODE_B".to_string(),
            requested_by: "".to_string(),
            gossip,
        };

        message_handler.handle_ping(&action).await.unwrap();

        assert_eq!(message_handler.membership_list.len(), 3);
        assert_eq!(
            message_handler.membership_list.member_incarnation("NODE_C"),
            Some(2)
        );
    }

    #[tokio::test]
    async fn test_message_handle_ping_with_node_joined_update_with_lt_incarnation_id() {
        let message_handler = create_message_handler().await;
        let gossip = Gossip {
            event: Some(Event::new_node_joined("NODE_A", "NODE_B", 0)),
        };

        let action = Ping {
            from: "NODE_B".to_string(),
            requested_by: "".to_string(),
            gossip: vec![gossip],
        };

        message_handler.handle_ping(&action).await.unwrap();

        assert_eq!(message_handler.membership_list.len(), 2);
        assert_eq!(
            message_handler.membership_list.member_incarnation("NODE_B"),
            Some(0)
        );
    }

    #[tokio::test]
    async fn test_message_handle_ping_with_node_joined_update_with_gt_incarnation_id() {
        let message_handler = create_message_handler().await;
        let gossip = Gossip {
            event: Some(Event::new_node_joined("NODE_A", "NODE_B", 6)),
        };

        let action = Ping {
            from: "NODE_B".to_string(),
            requested_by: "".to_string(),
            gossip: vec![gossip],
        };

        message_handler.handle_ping(&action).await.unwrap();

        assert_eq!(message_handler.membership_list.len(), 2);
        assert_eq!(
            message_handler.membership_list.member_incarnation("NODE_B"),
            Some(6)
        );
    }

    #[tokio::test]
    async fn test_message_handle_ping_with_node_joined_unknown_with_incarnation_id() {
        let message_handler = create_message_handler().await;
        let gossip = Gossip {
            event: Some(Event::new_node_joined("NODE_A", "NODE_C", 1)),
        };

        let action = Ping {
            from: "NODE_B".to_string(),
            requested_by: "".to_string(),
            gossip: vec![gossip],
        };

        message_handler.handle_ping(&action).await.unwrap();

        assert_eq!(message_handler.membership_list.len(), 3);
        assert!(message_handler
            .membership_list
            .members()
            .contains_key("NODE_C"));
        assert_eq!(
            message_handler.membership_list.member_incarnation("NODE_C"),
            Some(1)
        );
    }

    #[tokio::test]
    async fn test_message_handle_ping_with_node_joined() {
        let message_handler = create_message_handler().await;
        let gossip = Gossip {
            event: Some(Event::new_node_joined("NODE_A", "NODE_C", 0)),
        };

        let action = Ping {
            from: "NODE_B".to_string(),
            requested_by: "".to_string(),
            gossip: vec![gossip],
        };

        message_handler.handle_ping(&action).await.unwrap();

        assert_eq!(message_handler.membership_list.len(), 3);
        assert!(message_handler
            .membership_list
            .members()
            .contains_key("NODE_C"));
    }

    #[tokio::test]
    async fn test_message_handle_ack_with_forward() {
        let message_handler = create_message_handler().await;
        let gossip = message_handler
            .disseminator
            .get_gossip(message_handler.membership_list.len())
            .await;

        let action = Ack {
            from: "NODE_B".to_string(),
            forward_to: "NODE_C".to_string(),
            gossip: gossip.clone(),
        };

        message_handler.handle_ack(&action).await.unwrap();

        let result = &message_handler.socket.transmitted().await[0];

        let expected = SwimMessage {
            action: Some(Action::new_ack("NODE_B", "", gossip)),
        };

        assert_eq!(result, &expected);
    }

    #[tokio::test]
    async fn test_message_handle_ack_no_forward() {
        let message_handler = create_message_handler().await;

        message_handler
            .membership_list
            .update_member(Member::new("NODE_B", NodeState::Pending, 0));
        let gossip = message_handler
            .disseminator
            .get_gossip(message_handler.membership_list.len())
            .await;

        let action = Ack {
            from: "NODE_B".to_string(),
            forward_to: "".to_string(),
            gossip,
        };

        message_handler.handle_ack(&action).await.unwrap();

        let result = message_handler
            .membership_list
            .member_state("NODE_B")
            .unwrap()
            .unwrap();
        let expected = NodeState::Alive;

        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_message_handle_ping_req() {
        let message_handler = create_message_handler().await;
        let gossip = message_handler
            .disseminator
            .get_gossip(message_handler.membership_list.len())
            .await;

        let action = PingReq {
            from: "NODE_B".to_string(),
            suspect: "NODE_C".to_string(),
            gossip: gossip.clone(),
        };

        message_handler.handle_ping_req(&action).await.unwrap();

        let result = &message_handler.socket.transmitted().await[0];

        let expected = SwimMessage {
            action: Some(Action::new_ping("NODE_A", "NODE_B", gossip)),
        };

        assert_eq!(result, &expected);
    }

    #[tokio::test]
    async fn test_message_handle_ping_with_deceased_node() {
        let message_handler = create_message_handler().await;
        message_handler
            .disseminator
            .push(DisseminatorUpdate::NodesDeceased(Event::new_node_deceased(
                "NODE_A", "NODE_C", 3,
            )))
            .await;
        let gossip = message_handler
            .disseminator
            .get_gossip(message_handler.membership_list.len())
            .await;

        let action = Ping {
            from: "NODE_C".to_string(),
            requested_by: "".to_string(),
            gossip: gossip.clone(),
        };

        message_handler.handle_ping(&action).await.unwrap();

        assert_eq!(message_handler.membership_list.len(), 3);
        assert!(message_handler
            .membership_list
            .members()
            .contains_key("NODE_C"));
        assert_eq!(
            message_handler.membership_list.member_incarnation("NODE_C"),
            Some(4)
        );
    }

    #[tokio::test]
    async fn test_message_handle_ping_with_unknown_member() {
        let message_handler = create_message_handler().await;
        let gossip = message_handler
            .disseminator
            .get_gossip(message_handler.membership_list.len())
            .await;

        let action = Ping {
            from: "NODE_C".to_string(),
            requested_by: "".to_string(),
            gossip: gossip.clone(),
        };

        message_handler.handle_ping(&action).await.unwrap();

        assert_eq!(message_handler.membership_list.len(), 3);
        assert!(message_handler
            .membership_list
            .members()
            .contains_key("NODE_C"));
        assert_eq!(
            message_handler.membership_list.member_incarnation("NODE_C"),
            Some(0)
        );
    }

    #[tokio::test]
    async fn test_message_handle_ping() {
        let message_handler = create_message_handler().await;
        let gossip = message_handler
            .disseminator
            .get_gossip(message_handler.membership_list.len())
            .await;

        let action = Ping {
            from: "NODE_B".to_string(),
            requested_by: "".to_string(),
            gossip: gossip.clone(),
        };

        message_handler.handle_ping(&action).await.unwrap();

        let result = &message_handler.socket.transmitted().await[0];

        let expected = SwimMessage {
            action: Some(Action::new_ack("NODE_A", "", gossip)),
        };

        assert_eq!(result, &expected);
    }
}
