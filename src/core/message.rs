use std::sync::Arc;

use crate::{
    api::config::DEFAULT_BUFFER_SIZE,
    core::{disseminate::DisseminatorUpdate, utils::send_action},
    error::{Error, Result},
    pb::{
        gossip::{NodeJoined, NodeRecovered, NodeSuspected},
        swim_message::{self, Ack, Action, JoinRequest, JoinResponse, Ping, PingReq},
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
                JoinRequest(v) => self.handle_join_request(&v).await,
                JoinResponse(v) => self.handle_join_response(&v).await,
            },
            None => Err(Error::InvalidData {
                message: "Message must contain an 'action'".to_string(),
                location: location!(),
            }),
        }
    }

    pub(crate) async fn handle_ping(&self, action: &Ping) -> Result<()> {
        tracing::debug!("[{}] handling {action:?}", &self.addr);

        self.handle_gossip(&action.gossip).await;

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

    pub(crate) async fn handle_join_request(&self, action: &JoinRequest) -> Result<()> {
        tracing::debug!("[{}] handling {action:?}", &self.addr);

        let target = &action.from;
        self.membership_list.add_member(target, 0);

        let event = Event::NodeJoined(NodeJoined {
            from: self.addr.clone(),
            new_member: target.clone(),
        });

        self.disseminator
            .push(DisseminatorUpdate::NodesAlive(event.clone()))
            .await;

        if let Err(e) = self.tx.send(event) {
            tracing::error!("SendEventError: {}", e.to_string());
        }

        let members = self.membership_list.members_hashmap();
        let action = Action::JoinResponse(JoinResponse { members });

        send_action(&*self.socket, &action, target).await
    }

    pub(crate) async fn handle_join_response(&self, action: &JoinResponse) -> Result<()> {
        tracing::debug!("[{}] handling {action:?}", &self.addr);

        let iter = action.members.iter().map(|x| x.1.clone());
        self.membership_list.update_from_iter(iter)
    }

    pub(crate) async fn send_join_req(&self, target: impl AsRef<str>) -> Result<()> {
        let target = target.as_ref();

        let action = Action::JoinRequest(JoinRequest {
            from: self.addr.clone(),
        });

        tracing::debug!("[{}] sending JOIN_REQ to {}", &self.addr, target);
        send_action(&*self.socket, &action, target).await?;

        Ok(())
    }

    async fn handle_gossip(&self, gossip: &[Gossip]) {
        tracing::debug!("[{}] handling {:?}", &self.addr, gossip);
        for message in gossip {
            if let Some(event) = &message.event {
                match event {
                    Event::NodeJoined(evt) => self.handle_node_joined(evt),
                    Event::NodeRecovered(evt) => self.handle_node_recovered(evt).await,
                    Event::NodeSuspected(evt) => self.handle_node_suspected(evt).await,
                    Event::NodeDeceased(evt) => self.handle_node_deceased(evt),
                }
            }
        }
    }

    fn handle_node_joined(&self, event: &NodeJoined) {
        self.membership_list.add_member(&event.new_member, 0);

        let join_event = Event::NodeJoined(NodeJoined {
            from: self.addr.clone(),
            new_member: event.new_member.clone(),
        });
        if let Err(e) = self.tx.send(join_event) {
            tracing::error!("SendEventError: {}", e.to_string());
        }
    }

    async fn handle_node_recovered(&self, event: &NodeRecovered) {
        let incoming_incarnation = event.recovered_incarnation_no;
        let current_incarnation = self
            .membership_list
            .member_incarnation(&event.recovered)
            .unwrap_or(0);
        if incoming_incarnation > current_incarnation {
            self.membership_list.update_member(Member::new(
                &event.recovered,
                NodeState::Alive,
                event.recovered_incarnation_no,
            ));

            let recover_event = Event::NodeRecovered(NodeRecovered {
                from: self.addr.clone(),
                recovered: event.recovered.clone(),
                recovered_incarnation_no: event.recovered_incarnation_no,
            });

            if let Err(e) = self.tx.send(recover_event) {
                tracing::error!("SendEventError: {}", e.to_string());
            }
        }
    }

    async fn handle_node_suspected(&self, event: &NodeSuspected) {
        if event.suspect == self.addr {
            let recover_event = Event::NodeRecovered(NodeRecovered {
                from: self.addr.clone(),
                recovered: self.addr.clone(),
                recovered_incarnation_no: event.suspect_incarnation_no + 1,
            });

            self.disseminator
                .push(DisseminatorUpdate::NodesAlive(recover_event.clone()))
                .await;

            if let Err(e) = self.tx.send(recover_event) {
                tracing::error!("SendEventError: {}", e.to_string());
            }
        }

        let incoming_incarnation = event.suspect_incarnation_no;
        let current_incarnation = self
            .membership_list
            .member_incarnation(&event.suspect)
            .unwrap_or(0);

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
                event.suspect_incarnation_no,
            ));

            let suspect_event = Event::NodeSuspected(NodeSuspected {
                from: self.addr.clone(),
                suspect: event.suspect.clone(),
                suspect_incarnation_no: event.suspect_incarnation_no,
            });

            if let Err(e) = self.tx.send(suspect_event) {
                tracing::error!("SendEventError: {}", e.to_string());
            }
        }
    }

    fn handle_node_deceased(&self, event: &NodeDeceased) {
        self.membership_list.members().remove(&event.deceased);

        let deceased_event = Event::NodeDeceased(event.clone());
        if let Err(e) = self.tx.send(deceased_event) {
            tracing::error!("SendEventError: {}", e.to_string());
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
            gossip::{NodeDeceased, NodeJoined, NodeRecovered, NodeSuspected},
            swim_message::{Ack, Action, JoinRequest, JoinResponse, Ping, PingReq},
            Gossip, Member, NodeState, SwimMessage,
        },
        test_utils::mocks::MockUdpSocket,
        Event,
    };

    fn create_message_handler() -> MessageHandler<MockUdpSocket> {
        let socket = Arc::new(MockUdpSocket::new());

        let membership_list = Arc::new(MembershipList::new("NODE_A", 0));
        membership_list.add_member("NODE_B", 0);

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
        let message_handler = create_message_handler();

        let gossip = Gossip {
            event: Some(Event::NodeDeceased(NodeDeceased {
                from: "NODE_B".to_string(),
                deceased: "NODE_A".to_string(),
                deceased_incarnation_no: 0,
            })),
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
        let message_handler = create_message_handler();

        let gossip = Gossip {
            event: Some(Event::NodeSuspected(NodeSuspected {
                from: "NODE_B".to_string(),
                suspect: "NODE_A".to_string(),
                suspect_incarnation_no: 0,
            })),
        };

        let action = Ping {
            from: "NODE_B".to_string(),
            requested_by: "".to_string(),
            gossip: vec![gossip],
        };

        message_handler.handle_ping(&action).await.unwrap();

        let expected = message_handler
            .membership_list
            .member_state("NODE_A")
            .unwrap()
            .unwrap();
        assert_eq!(NodeState::Suspected, expected);
    }

    #[tokio::test]
    async fn test_message_handle_ping_with_node_recovered_lower_incarnation_id() {
        let message_handler = create_message_handler();
        message_handler.membership_list.update_member(Member::new(
            "NODE_B",
            NodeState::Suspected,
            99,
        ));

        let gossip = Gossip {
            event: Some(Event::NodeRecovered(NodeRecovered {
                from: "NODE_B".to_string(),
                recovered: "NODE_B".to_string(),
                recovered_incarnation_no: 1,
            })),
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
        let message_handler = create_message_handler();
        message_handler.membership_list.update_member(Member::new(
            "NODE_B",
            NodeState::Suspected,
            0,
        ));

        let gossip = Gossip {
            event: Some(Event::NodeRecovered(NodeRecovered {
                from: "NODE_B".to_string(),
                recovered: "NODE_B".to_string(),
                recovered_incarnation_no: 1,
            })),
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
        let message_handler = create_message_handler();
        let gossip = Gossip {
            event: Some(Event::NodeJoined(NodeJoined {
                from: "NODE_A".to_string(),
                new_member: "NODE_C".to_string(),
            })),
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
    async fn test_message_send_join_request() {
        let message_handler = create_message_handler();

        message_handler.send_join_req("NODE_B").await.unwrap();

        let result = &message_handler.socket.transmitted().await[0];

        let expected = SwimMessage {
            action: Some(Action::JoinRequest(JoinRequest {
                from: "NODE_A".to_string(),
            })),
        };

        assert_eq!(result, &expected);
    }

    #[tokio::test]
    async fn test_message_handle_join_response() {
        let message_handler = create_message_handler();

        let mut members = message_handler.membership_list.members_hashmap();
        members.insert(
            "NODE_C".to_string(),
            Member::new("NODE_C", NodeState::Alive, 0),
        );
        members.insert(
            "NODE_D".to_string(),
            Member::new("NODE_D", NodeState::Alive, 0),
        );

        let action = JoinResponse { members };

        message_handler.handle_join_response(&action).await.unwrap();

        let result = message_handler.membership_list.members();

        assert_eq!(result.len(), 4);
        assert!(result.contains_key("NODE_C"));
        assert!(result.contains_key("NODE_D"));
    }

    #[tokio::test]
    async fn test_message_handle_join_request() {
        let message_handler = create_message_handler();

        let action = JoinRequest {
            from: "NODE_C".to_string(),
        };

        message_handler.handle_join_request(&action).await.unwrap();

        assert!(message_handler
            .membership_list
            .members()
            .contains_key("NODE_C"));

        let result = &message_handler.socket.transmitted().await[0];

        let expected = SwimMessage {
            action: Some(Action::JoinResponse(JoinResponse {
                members: message_handler.membership_list.members_hashmap(),
            })),
        };

        assert_eq!(result, &expected);
    }

    #[tokio::test]
    async fn test_message_handle_ack_with_forward() {
        let message_handler = create_message_handler();
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
        let message_handler = create_message_handler();

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
        let message_handler = create_message_handler();
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
    async fn test_message_handle_ping() {
        let message_handler = create_message_handler();
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
