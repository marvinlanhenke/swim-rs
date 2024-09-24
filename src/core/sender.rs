use std::sync::Arc;

use tokio::net::UdpSocket;

use crate::config::SwimConfig;
use crate::core::utils::send_action;
use crate::error::Result;
use crate::pb::gossip::{Event, NodeJoinRequested};
use crate::pb::swim_message::{Action, Ping};
use crate::pb::Gossip;

use super::member::MembershipList;

#[derive(Clone, Debug)]
pub(crate) struct MessageSender {
    addr: String,
    socket: Arc<UdpSocket>,
    membership_list: Arc<MembershipList>,
    config: Arc<SwimConfig>,
}

impl MessageSender {
    pub(crate) fn new(
        addr: impl Into<String>,
        socket: Arc<UdpSocket>,
        membership_list: Arc<MembershipList>,
        config: Arc<SwimConfig>,
    ) -> Self {
        let addr = addr.into();

        Self {
            addr,
            socket,
            membership_list,
            config,
        }
    }

    pub(crate) async fn send_ping(&self) -> Result<()> {
        let mut ping = Ping {
            from: self.addr.clone(),
            requested_by: "".to_string(),
            gossip: None,
        };

        match self.membership_list.get_random_member_list(1).first() {
            Some((target, _)) => {
                let action = Action::Ping(ping);
                send_action(&self.socket, &action, &target).await?;
            }
            None => {
                if self.should_request_join() {
                    tracing::info!("[{}] sending JoinRequest", &self.addr);

                    if let Some(target) = self.config.known_peers().first() {
                        let gossip = Gossip {
                            event: Some(Event::NodeJoinRequested(NodeJoinRequested {
                                from: self.addr.clone(),
                            })),
                        };
                        ping.gossip = Some(gossip);

                        let action = Action::Ping(ping);

                        send_action(&self.socket, &action, &target).await?;
                    }
                }
            }
        };

        Ok(())
    }

    pub(crate) async fn send_ping_req(&self) {
        tracing::info!("Sending PING_REQ");
    }

    fn should_request_join(&self) -> bool {
        self.membership_list.members().len() == 1 && !self.config.known_peers().is_empty()
    }
}
