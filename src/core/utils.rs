use crate::error::Result;
use crate::pb::swim_message::Action;

use super::transport::TransportLayer;

/// A macro to emit an event and disseminate it through the disseminator.
///
/// This macro simplifies the process of emitting an event and pushing an update to the disseminator.
/// It logs the event, pushes the update to the disseminator, and sends the event through a broadcast channel.
macro_rules! emit_and_disseminate_event {
    ($this:expr, $event:expr, $update:path) => {
        let event = $event;
        tracing::debug!("[{}] emitting {:#?}", $this.addr, event);

        $this.disseminator.push($update(event.clone())).await;

        if let Err(e) = $this.tx.send(event) {
            tracing::debug!("SendEventError: {}", e.to_string());
        }
    };
}

pub(crate) use emit_and_disseminate_event;

/// Sends an `Action` to a target node over the transport layer.
///
/// This function encodes the provided `Action` into a byte buffer and sends it to the specified target
/// using the provided transport layer.
///
/// # Arguments
///
/// * `socket` - A reference to the transport layer implementing `TransportLayer`.
/// * `action` - The `Action` to send.
/// * `target` - The target node's address to send the action to.
///
/// # Returns
///
/// Returns `Ok(())` if the action was sent successfully, or an `Error` if sending failed.
///
/// # Errors
///
/// This function returns an error if encoding the action or sending it over the transport layer fails.
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
        pb::{swim_message::Action, SwimMessage},
        test_utils::mocks::MockUdpSocket,
    };

    #[tokio::test]
    async fn test_utils_send_action() {
        let socket = MockUdpSocket::new();
        let action = Action::new_ping("NODE_A", "", vec![]);

        send_action(&socket, &action, "NODE_B").await.unwrap();

        let result = &socket.transmitted().await[0];
        let expected = SwimMessage {
            action: Some(action),
        };

        assert_eq!(result, &expected);
    }
}
