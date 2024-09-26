use crate::error::Result;
use crate::pb::swim_message::Action;

use super::transport::TransportLayer;

pub(crate) async fn send_action<T: TransportLayer>(
    socket: &T,
    action: &Action,
    target: impl AsRef<str>,
) -> Result<()> {
    let mut buf = vec![];
    action.encode(&mut buf);

    socket.send_to(&buf, target.as_ref()).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        core::utils::send_action,
        pb::{
            swim_message::{Action, Ping},
            SwimMessage,
        },
        test_utils::mocks::MockUdpSocket,
    };

    #[tokio::test]
    async fn test_utils_send_action() {
        let socket = MockUdpSocket::new();
        let action = Action::Ping(Ping {
            from: "NODE_A".to_string(),
            requested_by: "".to_string(),
            gossip: None,
        });

        send_action(&socket, &action, "NODE_B").await.unwrap();

        let result = &socket.transmitted().await[0];
        let expected = SwimMessage {
            action: Some(action),
        };

        assert_eq!(result, &expected);
    }
}
