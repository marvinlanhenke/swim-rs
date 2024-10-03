use std::hash::Hash;

use crate::{
    pb::{
        swim_message::{Action, Ping, PingReq},
        Gossip,
    },
    Event, NodeDeceased, NodeJoined, NodeRecovered, NodeSuspected,
};

impl Action {
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
}

impl Event {
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
