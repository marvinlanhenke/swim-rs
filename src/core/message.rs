use std::sync::Arc;

use crate::{
    config::{SwimConfig, DEFAULT_BUFFER_SIZE},
    error::{Error, Result},
    pb::{
        swim_message::{self, Ack, Action, JoinRequest, JoinResponse, Ping, PingReq},
        Gossip, NodeState, SwimMessage,
    },
};

use prost::Message;
use snafu::location;
use tokio::{net::UdpSocket, sync::RwLock};

use super::member::MembershipList;

#[derive(Copy, Clone, Debug)]
pub(crate) enum AckType {
    PingAck,
    PingReqAck,
}

#[derive(Clone, Debug)]
pub(crate) enum MessageHandlerState {
    SendingPing,
    SendingPingReq { target: String },
    WaitingForAck { target: String, ack_type: AckType },
    DeclaringNodeAsDead { target: String },
}

#[derive(Clone, Debug)]
pub(crate) struct MessageHandler {
    addr: String,
    socket: Arc<UdpSocket>,
    state: Arc<RwLock<MessageHandlerState>>,
    config: Arc<SwimConfig>,
    membership_list: Arc<MembershipList>,
}

impl MessageHandler {
    pub(crate) fn new(
        addr: impl Into<String>,
        socket: Arc<UdpSocket>,
        config: Arc<SwimConfig>,
        membership_list: Arc<MembershipList>,
    ) -> Self {
        let addr = addr.into();
        let state = Arc::new(RwLock::new(MessageHandlerState::SendingPing));

        Self {
            addr,
            socket,
            state,
            config,
            membership_list,
        }
    }

    pub(crate) async fn state(&self) -> MessageHandlerState {
        let state = self.state.read().await;
        (*state).clone()
    }

    pub(crate) async fn send_ping(&self) -> Result<()> {
        let action = Action::Ping(Ping {
            from: self.addr.clone(),
            requested_by: "".to_string(),
            gossip: None,
        });

        if let Some((target, _)) = self.membership_list.get_random_member_list(1, None).first() {
            self.send_action(&action, &target).await?;

            self.membership_list
                .update_member(target, NodeState::Pending);

            let mut state = self.state.write().await;
            *state = MessageHandlerState::WaitingForAck {
                target: target.clone(),
                ack_type: AckType::PingAck,
            };
        };

        Ok(())
    }

    pub(crate) async fn send_ping_req(&self, suspect: impl Into<String>) -> Result<()> {
        let _action = Action::PingReq(PingReq {
            from: self.addr.clone(),
            suspect: suspect.into(),
        });

        Ok(())
    }

    pub(crate) async fn send_join_req(&self, target: &str) -> Result<()> {
        let action = Action::JoinRequest(JoinRequest {
            from: self.addr.clone(),
        });

        self.send_action(&action, target).await?;

        Ok(())
    }

    pub(crate) async fn wait_for_ack(
        &self,
        target: impl Into<String>,
        ack_type: &AckType,
    ) -> Result<()> {
        let target = target.into();

        match ack_type {
            AckType::PingAck => {
                tokio::time::sleep(self.config.ping_timeout()).await;

                let mut state = self.state.write().await;
                match self.membership_list.member_state(&target)? {
                    NodeState::Alive => *state = MessageHandlerState::SendingPing,
                    NodeState::Pending => *state = MessageHandlerState::SendingPingReq { target },
                    _ => {}
                };

                Ok(())
            }
            AckType::PingReqAck => {
                tokio::time::sleep(self.config.ping_req_timeout()).await;

                let mut state = self.state.write().await;
                match self.membership_list.member_state(&target)? {
                    NodeState::Alive => *state = MessageHandlerState::SendingPing,
                    NodeState::Suspected => {
                        *state = MessageHandlerState::DeclaringNodeAsDead { target }
                    }
                    _ => {}
                };

                Ok(())
            }
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
        tracing::info!("[{}] handling {action:?}", &self.addr);

        let from = self.addr.clone();
        let forward_to = action.requested_by.clone();
        let gossip = self.handle_gossip(action.gossip.as_ref());

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

        self.send_action(&message, target).await
    }

    pub(crate) async fn handle_ping_req(&self, action: &PingReq) -> Result<()> {
        tracing::info!("[{}] handling {action:?}", &self.addr);
        Ok(())
    }

    pub(crate) async fn handle_ack(&self, action: &Ack) -> Result<()> {
        tracing::info!("[{}] handling {action:?}", &self.addr);
        Ok(())
    }

    pub(crate) async fn handle_join_request(&self, action: &JoinRequest) -> Result<()> {
        tracing::info!("[{}] handling {action:?}", &self.addr);

        let target = &action.from;
        self.membership_list.add_member(target);

        let members = self.membership_list.members_hashmap();
        let action = Action::JoinResponse(JoinResponse { members });

        self.send_action(&action, target).await
    }

    pub(crate) async fn handle_join_response(&self, action: &JoinResponse) -> Result<()> {
        tracing::info!("[{}] handling {action:?}", &self.addr);

        let iter = action.members.iter().map(|x| (x.0.clone(), *x.1));
        self.membership_list.update_from_iter(iter)
    }

    fn handle_gossip(&self, _gossip: Option<&Gossip>) -> Option<Gossip> {
        todo!()
    }

    async fn send_action(&self, action: &Action, target: impl AsRef<str>) -> Result<()> {
        let mut buf = vec![];
        action.encode(&mut buf);

        self.socket.send_to(&buf, target.as_ref()).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {}
