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

async fn create_single_node_with_addr(ms: u64, known_peers: &[&str], addr: &str) -> SwimCluster {
    let duration = Duration::from_millis(ms);
    let config = SwimConfig::builder()
        .with_ping_interval(duration)
        .with_ping_timeout(duration)
        .with_ping_req_timeout(duration)
        .with_suspect_timeout(duration * 10)
        .with_known_peers(known_peers)
        .build();
    SwimCluster::try_new(addr, config).await.unwrap()
}

async fn create_single_node(ms: u64, known_peers: &[&str]) -> SwimCluster {
    let addr = "127.0.0.1:0";
    create_single_node_with_addr(ms, known_peers, addr).await
}

#[tokio::test]
async fn test_swim_cluster_node_recover_event() {
    let ms = 10;
    let node1 = create_single_node(ms, &[]).await;
    let node2 = create_single_node(ms, &[node1.addr()]).await;
    let node3 = create_single_node(ms, &[node1.addr()]).await;

    node1.run().await;
    node2.run().await;
    let handles = node3.run().await;

    let mut rx1 = node1.subscribe();
    let mut rx2 = node2.subscribe();
    let mut rx3 = node3.subscribe();

    assert_event!(Event::NodeJoined, rx1, 1000, |_| true);
    assert_event!(Event::NodeJoined, rx2, 1000, |_| true);
    assert_event!(Event::NodeJoined, rx3, 1000, |_| true);

    tracing::info!("[{}] is shutting down...", node3.addr());
    handles.0.abort();
    handles.1.abort();
    let node3_addr = node3.addr().to_string();
    drop(node3);
    drop(rx3);

    assert_event!(Event::NodeSuspected, rx1, 1000, |_| true);
    assert_event!(Event::NodeSuspected, rx2, 1000, |_| true);

    let node3 = create_single_node_with_addr(ms, &[node1.addr()], &node3_addr).await;
    node3.run().await;
    let mut rx3 = node3.subscribe();

    assert_event!(Event::NodeRecovered, rx1, 1000, |_| true);
    assert_event!(Event::NodeRecovered, rx2, 1000, |_| true);
    assert_event!(Event::NodeRecovered, rx3, 1000, |_| true);
}

#[tokio::test]
async fn test_swim_cluster_node_suspect_events() {
    let node1 = create_single_node(10, &[]).await;
    let node2 = create_single_node(10, &[node1.addr()]).await;
    let node3 = create_single_node(10, &[node1.addr()]).await;

    node1.run().await;
    node2.run().await;
    let handles = node3.run().await;

    let mut rx1 = node1.subscribe();
    let mut rx2 = node2.subscribe();
    let mut rx3 = node3.subscribe();

    assert_event!(Event::NodeJoined, rx1, 1000, |_| true);
    assert_event!(Event::NodeJoined, rx2, 1000, |_| true);
    assert_event!(Event::NodeJoined, rx3, 1000, |_| true);

    tracing::info!("[{}] is shutting down...", node3.addr());
    handles.0.abort();
    handles.1.abort();

    assert_event!(Event::NodeSuspected, rx1, 1000, |_| true);
    assert_event!(Event::NodeSuspected, rx2, 1000, |_| true);
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
