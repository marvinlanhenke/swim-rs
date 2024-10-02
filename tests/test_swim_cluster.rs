use std::time::Duration;

use swim_rs::{
    api::{config::SwimConfig, swim::SwimCluster},
    Event,
};

macro_rules! assert_event {
    ($event:path, $rx:expr, $ms:expr, $assertion:expr) => {
        let result = tokio::time::timeout(Duration::from_millis($ms), async {
            loop {
                match $rx.recv().await {
                    Ok(outer_event) => {
                        if let $event(inner_event) = outer_event {
                            if $assertion(inner_event) {
                                break;
                            } else {
                                panic!()
                            }
                        }
                    }
                    Err(_) => panic!(),
                }
            }
        })
        .await;

        if result.is_err() {
            panic!()
        }
    };
}

async fn create_single_node(ms: u64, known_peers: &[&str]) -> SwimCluster {
    let duration = Duration::from_millis(ms);
    let config = SwimConfig::builder()
        .with_ping_interval(duration)
        .with_ping_timeout(duration)
        .with_ping_req_timeout(duration)
        .with_suspect_timeout(duration)
        .with_known_peers(known_peers)
        .build();
    let addr = "127.0.0.1:0";
    SwimCluster::try_new(addr, config).await.unwrap()
}

#[tokio::test]
async fn test_swim_cluster_node_join_events() {
    let node1 = create_single_node(10, &[]).await;
    let node2 = create_single_node(10, &[node1.addr()]).await;
    let node3 = create_single_node(10, &[node1.addr()]).await;

    node1.run().await;
    node2.run().await;
    node3.run().await;

    let mut rx1 = node1.subscribe();
    let mut rx2 = node2.subscribe();
    let mut rx3 = node3.subscribe();

    assert_event!(Event::NodeJoined, rx1, 1000, |_| true);
    assert_event!(Event::NodeJoined, rx2, 1000, |_| true);
    assert_event!(Event::NodeJoined, rx3, 1000, |_| true);

    assert_eq!(node1.membership_list().len(), 3);
    assert_eq!(node2.membership_list().len(), 3);
    assert_eq!(node3.membership_list().len(), 3);
}
