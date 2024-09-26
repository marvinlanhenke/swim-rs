use std::sync::Arc;

use crate::{
    api::config::DEFAULT_BUFFER_SIZE,
    core::utils::send_action,
    error::{Error, Result},
    pb::{
        swim_message::{self, Ack, Action, JoinRequest, JoinResponse, Ping, PingReq},
        Gossip, NodeState, SwimMessage,
    },
};

use prost::Message;
use snafu::location;

use super::{member::MembershipList, transport::TransportLayer};

#[derive(Clone, Debug)]
pub(crate) struct MessageHandler<T: TransportLayer> {
    addr: String,
    socket: Arc<T>,
    membership_list: Arc<MembershipList>,
}

impl<T: TransportLayer> MessageHandler<T> {
    pub(crate) fn new(
        addr: impl Into<String>,
        socket: Arc<T>,
        membership_list: Arc<MembershipList>,
    ) -> Self {
        let addr = addr.into();

        Self {
            addr,
            socket,
            membership_list,
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

        let from = self.addr.clone();
        let forward_to = action.requested_by.clone();
        let gossip = None;

        let message = Action::Ack(Ack {
            from,
            forward_to,
            gossip,
        });

        // We check if the PING was `requested_by` PING_REQ,
        // if it was we send the ACK to the original issuer.
        let target = match action.requested_by.is_empty() {
            true => &action.from,
            false => &action.requested_by,
        };

        send_action(&*self.socket, &message, target).await
    }

    pub(crate) async fn handle_ping_req(&self, action: &PingReq) -> Result<()> {
        tracing::debug!("[{}] handling {action:?}", &self.addr);

        let requested_by = action.from.clone();
        let target = action.suspect.clone();

        let action = Action::Ping(Ping {
            from: self.addr.clone(),
            requested_by,
            gossip: None,
        });

        send_action(&*self.socket, &action, &target).await
    }

    pub(crate) async fn handle_ack(&self, action: &Ack) -> Result<()> {
        tracing::debug!("[{}] handling {action:?}", &self.addr);

        match action.forward_to.is_empty() {
            true => self
                .membership_list
                .update_member(&action.from, NodeState::Alive),
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
        self.membership_list.add_member(target);

        let members = self.membership_list.members_hashmap();
        let action = Action::JoinResponse(JoinResponse { members });

        send_action(&*self.socket, &action, target).await

        // TODO: add new nodes to disseminator, to update all other nodes in the cluster as well
    }

    pub(crate) async fn handle_join_response(&self, action: &JoinResponse) -> Result<()> {
        tracing::debug!("[{}] handling {action:?}", &self.addr);

        let iter = action.members.iter().map(|x| (x.0.clone(), *x.1));
        self.membership_list.update_from_iter(iter)
    }

    fn _handle_gossip(&self, _gossip: Option<&Gossip>) -> Option<Gossip> {
        todo!()
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        core::{member::MembershipList, message::MessageHandler},
        pb::{
            swim_message::{Ack, Action, JoinRequest, JoinResponse, Ping, PingReq},
            NodeState, SwimMessage,
        },
        test_utils::mocks::MockUdpSocket,
    };

    fn create_message_handler() -> MessageHandler<MockUdpSocket> {
        let socket = Arc::new(MockUdpSocket::new());

        let membership_list = Arc::new(MembershipList::new("NODE_A"));
        membership_list.add_member("NODE_B");

        MessageHandler::new("NODE_A", socket, membership_list)
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
        members.insert("NODE_C".to_string(), NodeState::Alive as i32);
        members.insert("NODE_D".to_string(), NodeState::Alive as i32);

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

        let action = Ack {
            from: "NODE_B".to_string(),
            forward_to: "NODE_C".to_string(),
            gossip: None,
        };

        message_handler.handle_ack(&action).await.unwrap();

        let result = &message_handler.socket.transmitted().await[0];

        let expected = SwimMessage {
            action: Some(Action::Ack(Ack {
                from: "NODE_B".to_string(),
                forward_to: "".to_string(),
                gossip: None,
            })),
        };

        assert_eq!(result, &expected);
    }

    #[tokio::test]
    async fn test_message_handle_ack_no_forward() {
        let message_handler = create_message_handler();

        message_handler
            .membership_list
            .update_member("NODE_B", NodeState::Pending);

        let action = Ack {
            from: "NODE_B".to_string(),
            forward_to: "".to_string(),
            gossip: None,
        };

        message_handler.handle_ack(&action).await.unwrap();

        let result = message_handler
            .membership_list
            .member_state("NODE_B")
            .unwrap();
        let expected = NodeState::Alive;

        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_message_handle_ping_req() {
        let message_handler = create_message_handler();

        let action = PingReq {
            from: "NODE_B".to_string(),
            suspect: "NODE_C".to_string(),
        };

        message_handler.handle_ping_req(&action).await.unwrap();

        let result = &message_handler.socket.transmitted().await[0];

        let expected = SwimMessage {
            action: Some(Action::Ping(Ping {
                from: "NODE_A".to_string(),
                requested_by: "NODE_B".to_string(),
                gossip: None,
            })),
        };

        assert_eq!(result, &expected);
    }

    #[tokio::test]
    async fn test_message_handle_ping() {
        let message_handler = create_message_handler();

        let action = Ping {
            from: "NODE_B".to_string(),
            requested_by: "".to_string(),
            gossip: None,
        };

        message_handler.handle_ping(&action).await.unwrap();

        let result = &message_handler.socket.transmitted().await[0];

        let expected = SwimMessage {
            action: Some(Action::Ack(Ack {
                from: "NODE_A".to_string(),
                forward_to: "".to_string(),
                gossip: None,
            })),
        };

        assert_eq!(result, &expected);
    }
}
