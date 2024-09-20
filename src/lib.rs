use prost::Message;
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use snafu::location;
use tokio::net::UdpSocket;

use crate::error::{Error, Result};
use std::{net::SocketAddr, sync::Arc, time::Duration};

pub mod error;

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/swim.rs"));
}

use pb::{gossip::GossipType, Gossip, JoinRequest, NodeId};

#[allow(unused)]
#[derive(Debug, PartialEq, Eq, Hash)]
enum NodeState {
    Alive,
    Suspect,
    Dead,
}

#[allow(unused)]
#[derive(Debug)]
pub struct SwimNode {
    addr: SocketAddr,
    peers: Option<Vec<SocketAddr>>,
    members: Arc<Vec<SocketAddr>>,
    socket: Arc<UdpSocket>,
}

impl SwimNode {
    pub async fn try_new(addr: &str, peers: Option<&[&str]>) -> Result<Self> {
        let addr = addr.parse()?;
        let peers = peers
            .map(|peer_list| {
                peer_list
                    .iter()
                    .map(|peer_str| {
                        peer_str.parse().map_err(|_| Error::AddrParse {
                            message: "failed to parse peer address".to_string(),
                            location: location!(),
                        })
                    })
                    .collect::<Result<Vec<SocketAddr>>>()
            })
            .transpose()?;

        let members = vec![addr];
        let socket = UdpSocket::bind(&addr).await?;

        Ok(Self {
            addr,
            peers,
            members: Arc::new(members),
            socket: Arc::new(socket),
        })
    }

    pub async fn run(&self) -> Result<()> {
        tracing::info!("SwimNode listening on {}", self.addr);

        // if peer fails to answer -> mark as peer as dead
        // log warning -> node could not connect to cluster -> best-practices to handle that?
        if let Some(peer_list) = &self.peers {
            if let Some(target) = peer_list.first() {
                self.send_join_request(target).await?;
            }
        }

        // self.send_message().await?;
        self.dispatch_message().await?;

        Ok(())
    }

    async fn dispatch_message(&self) -> Result<()> {
        let socket = self.socket.clone();
        let addr = self.addr;

        tokio::spawn(async move {
            let mut buf = [0u8; 1536];

            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((bytes, src)) => {
                        tracing::info!("[{}] received {} bytes from [{}]", addr, bytes, src);
                        let gossip = Gossip::decode(&buf[..bytes]);

                        match gossip {
                            Ok(msg) => match msg.gossip_type {
                                Some(GossipType::JoinRequest(req)) => tracing::info!("{:?}", req),
                                _ => {}
                            },
                            Err(e) => tracing::error!("[{}] DecodeError: {}", addr, e.to_string()),
                        }
                    }
                    Err(e) => {
                        tracing::error!("[{}] DispatchError: {}", addr, e.to_string())
                    }
                }
            }
        });

        Ok(())
    }

    async fn send_join_request(&self, target: &SocketAddr) -> Result<()> {
        let message = JoinRequest {
            from: Some(NodeId {
                addr: self.addr.to_string(),
            }),
        };

        let gossip = GossipType::JoinRequest(message);

        let mut buf = vec![];
        gossip.encode(&mut buf);

        self.socket.send_to(&buf, target).await?;

        Ok(())
    }

    // failure detection -> abstract
    async fn send_message(&self) -> Result<()> {
        let socket = self.socket.clone();
        let members = self.members.clone();

        let mut rng: StdRng = SeedableRng::from_entropy();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(1000));
            loop {
                interval.tick().await;
                let member = members.choose(&mut rng).cloned();
                if let Some(addr) = member {
                    let _ = socket.send_to(b"PING", addr).await;
                }
            }
        });

        Ok(())
    }
}
