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
                send_action(
                    &*self.socket,
                    &Action::Ack(action.clone()),
                    &action.forward_to,
                )
                .await?
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
mod tests {}
