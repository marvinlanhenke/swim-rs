use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use rand::{seq::IteratorRandom, thread_rng};
use snafu::location;
use tokio::sync::Notify;

use crate::error::{Error, Result};
use crate::pb::NodeState;

#[derive(Clone, Debug)]
pub struct MembershipList {
    addr: String,
    members: DashMap<String, NodeState>,
    notify: Arc<Notify>,
}

impl MembershipList {
    pub fn new(addr: impl Into<String>) -> Self {
        let addr = addr.into();
        let members = DashMap::from_iter([(addr.clone(), NodeState::Alive)]);
        let notify = Arc::new(Notify::new());

        Self {
            addr,
            members,
            notify,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn members(&self) -> &DashMap<String, NodeState> {
        &self.members
    }

    pub fn members_hashmap(&self) -> HashMap<String, i32> {
        self.members
            .iter()
            .map(|x| (x.key().clone(), *x.value() as i32))
            .collect()
    }

    pub fn member_state(&self, addr: impl AsRef<str>) -> Result<NodeState> {
        let addr = addr.as_ref();

        let state = match self.members.get(addr) {
            Some(state) => *state.value(),
            None => {
                return Err(Error::InvalidData {
                    message: format!("Node with addr {} is not a member", addr),
                    location: location!(),
                });
            }
        };

        Ok(state)
    }

    pub fn add_member(&self, addr: impl Into<String>) {
        self.members.insert(addr.into(), NodeState::Alive);
        self.notify_waiters();
    }

    pub fn update_member(&self, addr: impl Into<String>, state: NodeState) {
        self.members.insert(addr.into(), state);
        self.notify_waiters();
    }

    pub fn update_from_iter<I>(&self, iter: I) -> Result<()>
    where
        I: IntoIterator<Item = (String, i32)>,
    {
        for (key, value) in iter {
            let value = NodeState::try_from(value)?;
            self.members.insert(key, value);
        }

        self.notify_waiters();

        Ok(())
    }

    pub fn get_random_member_list(
        &self,
        amount: usize,
        exclude: Option<&str>,
    ) -> Vec<(String, NodeState)> {
        let mut rng = thread_rng();

        self.members
            .iter()
            .filter_map(|entry| {
                let key = entry.key();
                if key == &self.addr {
                    return None;
                }

                if let Some(exclude) = exclude {
                    if key == exclude {
                        return None;
                    }
                }

                Some((entry.key().clone(), *entry.value()))
            })
            .choose_multiple(&mut rng, amount)
    }

    pub async fn wait_for_members(&self) {
        while self.len() <= 1 {
            tracing::info!("[{}] waiting for members", self.addr);
            self.notify.notified().await;
        }
    }

    fn notify_waiters(&self) {
        if self.len() > 1 {
            self.notify.notify_waiters();
        }
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
        membership_list
            .update_from_iter([("127.0.0.1:8081".to_string(), NodeState::Alive as i32)])
            .unwrap();

        let random_members = membership_list.get_random_member_list(1, None);
        assert_eq!(random_members.len(), 1);

        let random_members = membership_list.get_random_member_list(2, None);
        assert_eq!(random_members.len(), 1);

        let random_members = membership_list.get_random_member_list(200, None);
        assert_eq!(random_members.len(), 1);
    }

    #[test]
    fn test_membershiplist_update_from_iter() {
        let iter = [
            ("127.0.0.1:8080".to_string(), NodeState::Suspected as i32),
            ("127.0.0.1:8081".to_string(), NodeState::Alive as i32),
            ("127.0.0.1:8082".to_string(), NodeState::Alive as i32),
        ];
        let addr = "127.0.0.1:8080";
        let membership_list = MembershipList::new(addr);
        membership_list.update_from_iter(iter).unwrap();

        let members = membership_list.members();

        assert_eq!(members.len(), 3);
        assert!(members.contains_key("127.0.0.1:8080"));
        assert_eq!(
            members.get("127.0.0.1:8080").unwrap().value(),
            &NodeState::Suspected
        );
    }
}
