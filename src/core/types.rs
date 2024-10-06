//! # SWIM Message Actions and Events
//!
//! This module provides constructors for SWIM message actions and events,
//! such as `Ping`, `PingReq`, `Ack`, and various node state events.
//! It also implements `Hash` and `Eq` for `Gossip` messages, allowing them
//! to be used in hash-based collections.
use std::hash::Hash;

use crate::{
    pb::{
        swim_message::{Ack, Action, Ping, PingReq},
        Gossip,
    },
    Event, NodeDeceased, NodeJoined, NodeRecovered, NodeSuspected,
};

impl Action {
    /// Creates a new `Ping` action message.
    ///
    /// # Arguments
    ///
    /// * `from` - The address of the node sending the ping.
    /// * `requested_by` - The address of the node that requested this ping, if any.
    /// * `gossip` - A vector of gossip messages to include.
    ///
    /// # Returns
    ///
    /// A new `Action::Ping` variant containing the provided information.
    pub(crate) fn new_ping(
        from: impl Into<String>,
        requested_by: impl Into<String>,
        gossip: Vec<Gossip>,
    ) -> Self {
        Self::Ping(Ping {
            from: from.into(),
            requested_by: requested_by.into(),
            gossip,
        })
    }

    /// Creates a new `PingReq` action message.
    ///
    /// # Arguments
    ///
    /// * `from` - The address of the node requesting the ping.
    /// * `suspect` - The address of the suspected node to ping.
    /// * `gossip` - A vector of gossip messages to include.
    ///
    /// # Returns
    ///
    /// A new `Action::PingReq` variant containing the provided information.
    pub(crate) fn new_ping_req(
        from: impl Into<String>,
        suspect: impl Into<String>,
        gossip: Vec<Gossip>,
    ) -> Self {
        Self::PingReq(PingReq {
            from: from.into(),
            suspect: suspect.into(),
            gossip,
        })
    }

    /// Creates a new `Ack` action message.
    ///
    /// # Arguments
    ///
    /// * `from` - The address of the node sending the acknowledgement.
    /// * `forward_to` - The address of the node to forward the acknowledgement to, if any.
    /// * `gossip` - A vector of gossip messages to include.
    ///
    /// # Returns
    ///
    /// A new `Action::Ack` variant containing the provided information.
    pub(crate) fn new_ack(
        from: impl Into<String>,
        forward_to: impl Into<String>,
        gossip: Vec<Gossip>,
    ) -> Self {
        Self::Ack(Ack {
            from: from.into(),
            forward_to: forward_to.into(),
            gossip,
        })
    }
}

impl Event {
    /// Creates a new `NodeJoined` event.
    ///
    /// # Arguments
    ///
    /// * `from` - The address of the node that observed the join.
    /// * `new_member` - The address of the new member that joined.
    /// * `joined_incarnation_no` - The incarnation number of the new member.
    ///
    /// # Returns
    ///
    /// A new `Event::NodeJoined` variant containing the provided information.
    pub(crate) fn new_node_joined(
        from: impl Into<String>,
        new_member: impl Into<String>,
        joined_incarnation_no: u64,
    ) -> Self {
        Event::NodeJoined(NodeJoined {
            from: from.into(),
            new_member: new_member.into(),
            joined_incarnation_no,
        })
    }

    /// Creates a new `NodeSuspected` event.
    ///
    /// # Arguments
    ///
    /// * `from` - The address of the node that suspects another node.
    /// * `suspect` - The address of the suspected node.
    /// * `suspect_incarnation_no` - The incarnation number of the suspected node.
    ///
    /// # Returns
    ///
    /// A new `Event::NodeSuspected` variant containing the provided information.
    pub(crate) fn new_node_suspected(
        from: impl Into<String>,
        suspect: impl Into<String>,
        suspect_incarnation_no: u64,
    ) -> Self {
        Event::NodeSuspected(NodeSuspected {
            from: from.into(),
            suspect: suspect.into(),
            suspect_incarnation_no,
        })
    }

    /// Creates a new `NodeRecovered` event.
    ///
    /// # Arguments
    ///
    /// * `from` - The address of the node that observed the recovery.
    /// * `recovered` - The address of the node that has recovered.
    /// * `recovered_incarnation_no` - The new incarnation number of the recovered node.
    ///
    /// # Returns
    ///
    /// A new `Event::NodeRecovered` variant containing the provided information.
    pub(crate) fn new_node_recovered(
        from: impl Into<String>,
        recovered: impl Into<String>,
        recovered_incarnation_no: u64,
    ) -> Self {
        Event::NodeRecovered(NodeRecovered {
            from: from.into(),
            recovered: recovered.into(),
            recovered_incarnation_no,
        })
    }

    /// Creates a new `NodeDeceased` event.
    ///
    /// # Arguments
    ///
    /// * `from` - The address of the node that observed the node as deceased.
    /// * `deceased` - The address of the deceased node.
    /// * `deceased_incarnation_no` - The incarnation number of the deceased node.
    ///
    /// # Returns
    ///
    /// A new `Event::NodeDeceased` variant containing the provided information.
    pub(crate) fn new_node_deceased(
        from: impl Into<String>,
        deceased: impl Into<String>,
        deceased_incarnation_no: u64,
    ) -> Self {
        Event::NodeDeceased(NodeDeceased {
            from: from.into(),
            deceased: deceased.into(),
            deceased_incarnation_no,
        })
    }
}

impl Eq for Gossip {}

impl Hash for Gossip {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self.event {
            Some(Event::NodeJoined(e)) => {
                0u8.hash(state);
                e.hash(state);
            }
            Some(Event::NodeSuspected(e)) => {
                1u8.hash(state);
                e.hash(state);
            }
            Some(Event::NodeRecovered(e)) => {
                2u8.hash(state);
                e.hash(state);
            }
            Some(Event::NodeDeceased(e)) => {
                3u8.hash(state);
                e.hash(state);
            }
            None => {
                4u8.hash(state);
            }
        }
    }
}
