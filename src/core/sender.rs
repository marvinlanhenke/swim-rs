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
        if self.should_request_join() {
            tracing::info!("[{}] sending JoinRequest", &self.addr);

            let from = self.addr.clone();
            let requested_by = "".to_string();
            let gossip = Some(Gossip {
                event: Some(Event::NodeJoinRequested(NodeJoinRequested {
                    from: from.clone(),
                })),
            });

            let action = Action::Ping(Ping {
                from,
                requested_by,
                gossip,
            });

            let target = self
                .config
                .known_peers()
                .first()
                .expect("should not be empty");

            send_action(&self.socket, &action, &target).await?;

            return Ok(());
        }

        let from = self.addr.clone();
        let requested_by = "".to_string();
        let gossip = None;

        let action = Action::Ping(Ping {
            from,
            requested_by,
            gossip,
        });

        if let Some((target, _)) = self.membership_list.get_random_member_list(1).first() {
            send_action(&self.socket, &action, &target).await?;
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
