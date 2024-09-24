use dashmap::{DashMap, DashSet};
use rand::{seq::IteratorRandom, thread_rng};

use crate::pb::NodeState;

#[derive(Clone, Debug)]
pub(crate) struct MembershipList {
    addr: String,
    members: DashMap<String, NodeState>,
    pending: DashSet<String>,
    suspects: DashSet<String>,
}

impl MembershipList {
    pub(crate) fn new(addr: impl Into<String>) -> Self {
        let addr = addr.into();
        let members = DashMap::from_iter([(addr.clone(), NodeState::Alive)]);

        Self {
            addr,
            members,
            pending: DashSet::new(),
            suspects: DashSet::new(),
        }
    }

    pub(crate) fn members(&self) -> &DashMap<String, NodeState> {
        &self.members
    }

    pub(crate) fn update_from_iter<I>(&self, iter: I)
    where
        I: IntoIterator<Item = (String, NodeState)>,
    {
        for (key, value) in iter {
            self.members.insert(key, value);
        }
    }

    pub(crate) fn get_random_member_list(&self, amount: usize) -> Vec<(String, NodeState)> {
        let mut rng = thread_rng();

        self.members
            .iter()
            .filter_map(|entry| {
                let key = entry.key();
                if key == &self.addr || self.pending.contains(key) || self.suspects.contains(key) {
                    return None;
                }

                Some((entry.key().clone(), *entry.value()))
            })
            .choose_multiple(&mut rng, amount)
    }

    pub(crate) fn pending(&self) -> &DashSet<String> {
        &self.pending
    }

    pub(crate) fn add_pending(&self, key: impl Into<String>) -> bool {
        self.pending.insert(key.into())
    }

    pub(crate) fn remove_pending(&self, key: impl AsRef<str>) -> Option<String> {
        self.pending.remove(key.as_ref())
    }

    pub(crate) fn suspects(&self) -> &DashSet<String> {
        &self.suspects
    }

    pub(crate) fn add_suspect(&self, key: impl Into<String>) -> bool {
        self.suspects.insert(key.into())
    }

    pub(crate) fn remove_suspect(&self, key: impl AsRef<str>) -> Option<String> {
        self.suspects.remove(key.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use crate::pb::NodeState;

    use super::MembershipList;

    #[test]
    fn test_membershiplist_get_random_member() {
        let addr = "127.0.0.1:8080";
        let membership_list = MembershipList::new(addr);
        membership_list.update_from_iter([("127.0.0.1:8081".to_string(), NodeState::Alive)]);

        let random_members = membership_list.get_random_member_list(1);
        assert_eq!(random_members.len(), 1);

        let random_members = membership_list.get_random_member_list(2);
        assert_eq!(random_members.len(), 1);

        let random_members = membership_list.get_random_member_list(200);
        assert_eq!(random_members.len(), 1);
    }

    #[test]
    fn test_membershiplist_add_remove_suspects() {
        let addr = "127.0.0.1:8080";
        let membership_list = MembershipList::new(addr);

        membership_list.add_suspect("127.0.0.1:8081");
        membership_list.add_suspect("127.0.0.1:8082");
        assert_eq!(membership_list.suspects().len(), 2);

        membership_list.remove_suspect("127.0.0.1:8081");
        assert_eq!(membership_list.suspects().len(), 1);
    }

    #[test]
    fn test_membershiplist_add_remove_pending() {
        let addr = "127.0.0.1:8080";
        let membership_list = MembershipList::new(addr);

        membership_list.add_pending("127.0.0.1:8081");
        membership_list.add_pending("127.0.0.1:8082");
        assert_eq!(membership_list.pending().len(), 2);

        membership_list.remove_pending("127.0.0.1:8081");
        assert_eq!(membership_list.pending().len(), 1);
    }

    #[test]
    fn test_membershiplist_update_from_iter() {
        let iter = [
            ("127.0.0.1:8080".to_string(), NodeState::Suspected),
            ("127.0.0.1:8081".to_string(), NodeState::Alive),
            ("127.0.0.1:8082".to_string(), NodeState::Alive),
        ];
        let addr = "127.0.0.1:8080";
        let membership_list = MembershipList::new(addr);
        membership_list.update_from_iter(iter);

        let members = membership_list.members();

        assert_eq!(members.len(), 3);
        assert!(members.contains_key("127.0.0.1:8080"));
        assert_eq!(
            members.get("127.0.0.1:8080").unwrap().value(),
            &NodeState::Suspected
        );
    }
}
