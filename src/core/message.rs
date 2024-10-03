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

#[derive(Clone, Debug)]
pub(crate) struct MessageHandler<T: TransportLayer> {
    addr: String,
    socket: Arc<T>,
    membership_list: Arc<MembershipList>,
    disseminator: Arc<Disseminator>,
    tx: Sender<Event>,
}

impl<T: TransportLayer> MessageHandler<T> {
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

    pub(crate) async fn handle_ping(&self, action: &Ping) -> Result<()> {
        tracing::debug!("[{}] handling {action:#?}", &self.addr);

        self.handle_gossip(&action.gossip).await;

        if self.membership_list.add_member(&action.from, 0).await {
            emit_and_disseminate_event!(
                &self,
                Event::new_node_joined(&self.addr, &action.from),
                DisseminatorUpdate::NodesAlive
            );
        }

        // TODO: refactor actions with ::new
        let from = self.addr.clone();
        let forward_to = action.requested_by.clone();
        let gossip = self
            .disseminator
            .get_gossip(self.membership_list.len())
            .await;
        let message = Action::Ack(Ack {
            from,
            forward_to,
            gossip,
        });

        // TODO: refactor into Self::get_ack_target
        // We check if the PING was `requested_by` a PING_REQ,
        // if it was we send the ACK to the original issuer.
        let target = match action.requested_by.is_empty() {
            true => &action.from,
            false => &action.requested_by,
        };

        send_action(&*self.socket, &message, target).await
    }

    pub(crate) async fn handle_ping_req(&self, action: &PingReq) -> Result<()> {
        tracing::debug!("[{}] handling {action:?}", &self.addr);

        self.handle_gossip(&action.gossip).await;

        let requested_by = action.from.clone();
        let target = action.suspect.clone();
        let gossip = self
            .disseminator
            .get_gossip(self.membership_list.len())
            .await;

        let action = Action::Ping(Ping {
            from: self.addr.clone(),
            requested_by,
            gossip,
        });

        send_action(&*self.socket, &action, &target).await
    }

    pub(crate) async fn handle_ack(&self, action: &Ack) -> Result<()> {
        tracing::debug!("[{}] handling {action:?}", &self.addr);

        self.handle_gossip(&action.gossip).await;

        let member = match self.membership_list.members().get(&action.from) {
            Some(member) => member.value().clone(),
            None => {
                return Ok(());
            }
        };

        match action.forward_to.is_empty() {
            true => self.membership_list.update_member(Member::new(
                &action.from,
                NodeState::Alive,
                member.incarnation,
            )),
            false => {
                let target = &action.forward_to;
                let mut forwarded_ack = action.clone();
                forwarded_ack.forward_to = "".to_string();

                send_action(&*self.socket, &Action::Ack(forwarded_ack), target).await?
            }
        };

        Ok(())
    }

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

    async fn handle_node_joined(&self, event: &NodeJoined) {
        if self.membership_list.add_member(&event.new_member, 0).await {
            emit_and_disseminate_event!(
                &self,
                Event::new_node_joined(&self.addr, &event.new_member),
                DisseminatorUpdate::NodesAlive
            );
        }
        tracing::debug!("[{}] {:#?}", &self.addr, self.disseminator);
    }

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

    async fn handle_node_deceased(&self, event: &NodeDeceased) {
        let deceased = &event.deceased;

        if self.membership_list.members().contains_key(deceased) {
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

            tracing::debug!("[{}] {:#?}", &self.addr, self.disseminator);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::broadcast;

    use crate::{
        api::config::{DEFAULT_BUFFER_SIZE, DEFAULT_GOSSIP_MAX_MESSAGES},
        core::{disseminate::Disseminator, member::MembershipList, message::MessageHandler},
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
    async fn test_message_handle_ping_with_node_joined() {
        let message_handler = create_message_handler().await;
        let gossip = Gossip {
            event: Some(Event::new_node_joined("NODE_A", "NODE_C")),
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
            action: Some(Action::Ack(Ack {
                from: "NODE_B".to_string(),
                forward_to: "".to_string(),
                gossip,
            })),
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
            action: Some(Action::Ping(Ping {
                from: "NODE_A".to_string(),
                requested_by: "NODE_B".to_string(),
                gossip,
            })),
        };

        assert_eq!(result, &expected);
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
            action: Some(Action::Ack(Ack {
                from: "NODE_A".to_string(),
                forward_to: "".to_string(),
                gossip,
            })),
        };

        assert_eq!(result, &expected);
    }
}
