use prost::Message;
use snafu::location;
use tokio::{net::UdpSocket, sync::RwLock};

use crate::error::{Error, Result};
use std::{net::SocketAddr, sync::Arc, time::Duration};

pub mod error;

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/swim.rs"));
}

use pb::{gossip::GossipType, Gossip, JoinRequest, JoinResponse, NodeId};

#[allow(unused)]
#[derive(Debug, PartialEq, Eq, Hash)]
enum NodeState {
    Alive,
    Suspect,
    Dead,
}

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct SwimNode {
    addr: SocketAddr,
    peers: Option<Vec<SocketAddr>>,
    members: Arc<RwLock<Vec<NodeId>>>,
    socket: Arc<UdpSocket>,
}

impl SwimNode {
    pub async fn try_new(addr: &str, peers: Option<&[&str]>) -> Result<Self> {
        let addr: SocketAddr = addr.parse()?;
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

        let socket = UdpSocket::bind(&addr).await?;
        let members = vec![NodeId {
            addr: addr.to_string(),
        }];

        Ok(Self {
            addr,
            peers,
            members: Arc::new(RwLock::new(members)),
            socket: Arc::new(socket),
        })
    }

    pub fn members(&self) {
        tracing::info!("[{}] current members: {:?}", &self.addr, &self.members);
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
        self.check_members().await?;
        self.dispatch_message().await?;

        Ok(())
    }

    async fn dispatch_message(&self) -> Result<()> {
        let this = self.clone();

        tokio::spawn(async move {
            let mut buf = [0u8; 1536];

            loop {
                match this.socket.recv_from(&mut buf).await {
                    Ok((bytes, src)) => {
                        tracing::info!("[{}] received {} bytes from [{}]", this.addr, bytes, src);
                        let gossip = Gossip::decode(&buf[..bytes]);

                        match gossip {
                            Ok(msg) => match msg.gossip_type {
                                Some(GossipType::JoinRequest(req)) => {
                                    if let Some(from) = &req.from {
                                        let _ = Self::handle_join_request(&this, from).await;
                                    }
                                }
                                Some(GossipType::JoinResponse(req)) => {
                                    let _ = Self::handle_join_response(&this, &req).await;
                                }
                                Some(GossipType::Ping(_req)) => todo!(),
                                _ => {}
                            },
                            Err(e) => {
                                tracing::error!("[{}] DecodeError: {}", &this.addr, e.to_string())
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("[{}] DispatchError: {}", &this.addr, e.to_string())
                    }
                };
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

    async fn handle_join_request(this: &SwimNode, from: &NodeId) -> Result<()> {
        tracing::info!("[{}] handling JoinRequest from: [{}]", this.addr, from.addr);

        let mut members = this.members.write().await;
        members.push(NodeId {
            addr: from.addr.clone(),
        });

        let gossip = GossipType::JoinResponse(JoinResponse {
            member: members.to_vec(),
        });

        let mut buf = vec![];
        gossip.encode(&mut buf);

        for member in members.iter() {
            if member.addr == this.addr.to_string() {
                continue;
            }
            this.socket.send_to(&buf, &member.addr).await?;
        }

        Ok(())
    }

    async fn handle_join_response(this: &SwimNode, response: &JoinResponse) -> Result<()> {
        tracing::info!("[{}] handling JoinResponse {:?}", this.addr, response);

        let mut members = this.members.write().await;
        *members = response.member.clone();

        Ok(())
    }

    async fn check_members(&self) -> Result<()> {
        let this = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(2000));
            loop {
                interval.tick().await;
                this.members();
            }
        });

        Ok(())
    }

    // failure detection -> abstract
    // async fn send_message(&self) -> Result<()> {
    //     let socket = self.socket.clone();
    //     let members = self.members.clone();

    //     let mut rng: StdRng = SeedableRng::from_entropy();

    //     tokio::spawn(async move {
    //         let mut interval = tokio::time::interval(Duration::from_millis(1000));
    //         loop {
    //             interval.tick().await;
    //             let member = members.choose(&mut rng).cloned();
    //             if let Some(addr) = member {
    //                 let _ = socket.send_to(b"PING", addr).await;
    //             }
    //         }
    //     });

    //     Ok(())
    // }
}
