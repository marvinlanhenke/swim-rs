use crate::{Event, NodeDeceased, NodeJoined, NodeRecovered, NodeSuspected};

impl Event {
    pub(crate) fn new_node_joined(from: impl Into<String>, new_member: impl Into<String>) -> Self {
        Event::NodeJoined(NodeJoined {
            from: from.into(),
            new_member: new_member.into(),
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
