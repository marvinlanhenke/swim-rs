use std::time::Duration;

use swim_rs::{
    api::{config::SwimConfig, swim::SwimCluster},
    Event::{self},
    NodeDeceased, NodeJoined, NodeRecovered, NodeState, NodeSuspected,
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

fn create_config_with_duration(duration: Duration) -> SwimConfig {
    SwimConfig::builder()
        .with_ping_interval(duration)
        .with_ping_timeout(duration)
        .with_ping_req_timeout(duration)
        .with_suspect_timeout(duration)
        .build()
}

#[tokio::test]
async fn test_swim_multiple_nodes_joined_event() {
    let config = create_config_with_duration(Duration::from_millis(10));
    let seed = SwimCluster::try_new("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    let join1 = SwimCluster::try_new(
        "127.0.0.1:0",
        SwimConfig::builder()
            .with_known_peers(&[seed.addr()])
            .build(),
    )
    .await
    .unwrap();
    let join2 = SwimCluster::try_new(
        "127.0.0.1:0",
        SwimConfig::builder()
            .with_known_peers(&[seed.addr()])
            .build(),
    )
    .await
    .unwrap();

    seed.run().await;
    join1.run().await;
    join2.run().await;

    let mut rx_seed = seed.subscribe();
    let mut rx_join1 = join1.subscribe();
    let mut rx_join2 = join2.subscribe();

    assert_event!(Event::NodeJoined, rx_seed, 3000, |_| true);
    assert_event!(Event::NodeJoined, rx_join1, 3000, |_| true);
    assert_event!(Event::NodeJoined, rx_join2, 3000, |_| true);
}

#[tokio::test]
async fn test_swim_node_recovered_event() {
    let config = create_config_with_duration(Duration::from_millis(10));
    let node1 = SwimCluster::try_new("127.0.0.1:0", config).await.unwrap();
    let node2 = SwimCluster::try_new("127.0.0.1:0", SwimConfig::new())
        .await
        .unwrap();
    node1.membership_list().add_member(node2.addr(), 0).await;

    node1.run().await;

    let mut rx1 = node1.subscribe();
    let mut _rx2 = node2.subscribe();

    loop {
        match rx1.recv().await {
            Ok(Event::NodeSuspected(_)) => {
                node2.run().await;
                break;
            }
            Ok(_) => continue,
            Err(_) => panic!(),
        }
    }

    assert_event!(Event::NodeRecovered, rx1, 3000, |event: NodeRecovered| {
        event.from == node1.addr()
            && event.recovered == node2.addr()
            && event.recovered_incarnation_no > 0
    });

    let result = node1
        .membership_list()
        .member_state(node2.addr())
        .unwrap()
        .unwrap();
    assert_eq!(result, NodeState::Alive);
}

#[tokio::test]
async fn test_swim_node_joined_event() {
    let config = create_config_with_duration(Duration::from_millis(10));
    let node1 = SwimCluster::try_new("127.0.0.1:0", config).await.unwrap();
    let node2 = SwimCluster::try_new(
        "127.0.0.1:0",
        SwimConfig::builder()
            .with_known_peers(&[node1.addr()])
            .build(),
    )
    .await
    .unwrap();

    node1.run().await;
    node2.run().await;

    let mut rx1 = node1.subscribe();
    let mut rx2 = node2.subscribe();

    assert_event!(Event::NodeJoined, rx1, 3000, |event: NodeJoined| {
        event.from == node1.addr() && event.new_member == node2.addr()
    });

    assert_event!(Event::NodeJoined, rx2, 3000, |event: NodeJoined| {
        event.from == node2.addr() && event.new_member == node2.addr()
    });

    assert_eq!(node1.membership_list().len(), 2);
    assert_eq!(node2.membership_list().len(), 2);
}

#[tokio::test]
async fn test_swim_node_deceased_event() {
    let config = create_config_with_duration(Duration::from_millis(10));
    let node1 = SwimCluster::try_new("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    let node2 = SwimCluster::try_new("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    node1.membership_list().add_member(node2.addr(), 0).await;

    node1.run().await;

    let mut rx1 = node1.subscribe();
    let mut _rx2 = node2.subscribe();

    assert_event!(Event::NodeDeceased, rx1, 3000, |event: NodeDeceased| {
        event.from == node1.addr()
            && event.deceased == node2.addr()
            && event.deceased_incarnation_no == 0
    });

    assert_eq!(node1.membership_list().len(), 1);
}

#[tokio::test]
async fn test_swim_node_suspect_event() {
    let config = create_config_with_duration(Duration::from_millis(10));
    let node1 = SwimCluster::try_new("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    let node2 = SwimCluster::try_new("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    node1.membership_list().add_member(node2.addr(), 0).await;

    node1.run().await;

    let mut rx1 = node1.subscribe();
    let mut _rx2 = node2.subscribe();

    assert_event!(Event::NodeSuspected, rx1, 3000, |event: NodeSuspected| {
        event.from == node1.addr()
            && event.suspect == node2.addr()
            && event.suspect_incarnation_no == 0
    });

    let result = node1
        .membership_list()
        .member_state(node2.addr())
        .unwrap()
        .unwrap();
    assert_eq!(result, NodeState::Suspected);
}
