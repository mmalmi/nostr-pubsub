//! Relay control messages for applications that own reconciliation and durable storage.

use nostr_pubsub::{PubsubError, Result, VerifiedEvent};
use nostr_sdk::{
    Client, ClientMessage, ClientOptions, RelayMessage,
    prelude::{Relay, RelayNotification, RelayStatus, SubscribeOptions},
};
use tokio::sync::broadcast;

/// A message from exactly one relay. An `OK` is remote evidence; `send` only queues.
#[derive(Debug)]
pub enum RelaySessionEvent {
    Connection(bool),
    Message(Box<RelayMessage<'static>>),
    /// The consumer must reconcile after falling behind instead of assuming complete history.
    Lagged(u64),
}

/// A relay connection with shared reconnect, heartbeat, and subscription replay.
///
/// Events are verified and checked against their subscription. Control messages
/// (including NIP-77) retain their exact subscription IDs and relay provenance.
/// This owns no account keys, persistent event store, or publication outbox.
/// Dropping it closes its connection, including when a caller's deadline expires.
pub struct RelaySession {
    relay: Relay,
    _client: Client,
    notifications: broadcast::Receiver<RelayNotification>,
    connected: bool,
}

impl RelaySession {
    pub async fn connect(url: &str) -> Result<Self> {
        let client = Client::builder()
            .opts(
                ClientOptions::new()
                    .automatic_authentication(false)
                    .verify_subscriptions(true),
            )
            .build();
        client.add_relay(url).await.map_err(transport_error)?;
        let relay = client.relay(url).await.map_err(transport_error)?;
        let notifications = relay.notifications();
        relay.connect();
        Ok(Self {
            relay,
            _client: client,
            notifications,
            connected: false,
        })
    }

    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.relay.is_connected()
    }

    /// Queues a typed message. Only a subsequent matching relay `OK` acknowledges publication.
    /// `REQ` and `CLOSE` update the shared transport's desired subscriptions for reconnect.
    pub async fn send(&self, message: ClientMessage<'_>) -> Result<()> {
        match message {
            ClientMessage::Req {
                subscription_id,
                filters,
            } => self
                .relay
                .subscribe_with_id(
                    subscription_id.into_owned(),
                    filters
                        .into_iter()
                        .map(std::borrow::Cow::into_owned)
                        .collect::<Vec<_>>(),
                    SubscribeOptions::default(),
                )
                .await
                .map_err(transport_error),
            ClientMessage::Close(id) => self.relay.unsubscribe(&id).await.map_err(transport_error),
            ClientMessage::Event(ref event) | ClientMessage::Auth(ref event) => {
                VerifiedEvent::try_from(event.as_ref().clone())?;
                self.relay.send_msg(message).map_err(transport_error)
            }
            other => self.relay.send_msg(other).map_err(transport_error),
        }
    }

    pub async fn next(&mut self) -> Result<Option<RelaySessionEvent>> {
        loop {
            match self.notifications.recv().await {
                Ok(RelayNotification::RelayStatus { status }) => {
                    let connected = status == RelayStatus::Connected;
                    if (connected && !self.connected)
                        || matches!(
                            status,
                            RelayStatus::Disconnected
                                | RelayStatus::Terminated
                                | RelayStatus::Banned
                        )
                    {
                        self.connected = connected;
                        return Ok(Some(RelaySessionEvent::Connection(connected)));
                    }
                }
                Ok(RelayNotification::Message { message }) => {
                    if let RelayMessage::Event { event, .. } = &message
                        && VerifiedEvent::try_from(event.as_ref().clone()).is_err()
                    {
                        // A relay can reuse a known ID with a different payload. Its
                        // invalid event must not poison unrelated live interests.
                        continue;
                    }
                    return Ok(Some(RelaySessionEvent::Message(Box::new(message))));
                }
                Ok(RelayNotification::Shutdown) | Err(broadcast::error::RecvError::Closed) => {
                    return Ok(None);
                }
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    return Ok(Some(RelaySessionEvent::Lagged(count)));
                }
                Ok(_) => {}
            }
        }
    }
}

impl Drop for RelaySession {
    fn drop(&mut self) {
        self.relay.disconnect();
    }
}

fn transport_error(error: impl std::fmt::Display) -> PubsubError {
    PubsubError::Storage(format!("relay session: {error}"))
}

#[cfg(test)]
mod tests;
