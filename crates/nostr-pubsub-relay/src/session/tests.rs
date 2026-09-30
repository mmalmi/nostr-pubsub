use super::*;
use futures_util::{SinkExt, StreamExt};
use nostr_sdk::{EventBuilder, Filter, JsonUtil, Keys, Kind, SubscriptionId};
use std::time::Duration;
use tokio::{net::TcpListener, time::timeout};
use tokio_tungstenite::{accept_async, tungstenite::Message};

async fn connected(session: &mut RelaySession) {
    timeout(Duration::from_secs(25), async {
        while !matches!(
            session.next().await.unwrap(),
            Some(RelaySessionEvent::Connection(true))
        ) {}
    })
    .await
    .expect("relay connects or reconnects");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn preserves_control_frames_and_filters_event_admission_on_one_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let event = EventBuilder::new(Kind::TextNote, "accepted")
        .sign_with_keys(&Keys::generate())
        .unwrap();
    let wanted = event.id;
    let mut forged = event.clone();
    forged.content = "invalid signature".into();
    let wrong = EventBuilder::new(Kind::Metadata, "{}")
        .sign_with_keys(&Keys::generate())
        .unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        loop {
            let message = socket.next().await.unwrap().unwrap();
            let Ok(text) = message.to_text() else {
                continue;
            };
            if let Ok(ClientMessage::Req {
                subscription_id,
                filters,
            }) = ClientMessage::from_json(text)
            {
                assert_eq!(subscription_id.as_str(), "interest");
                assert_eq!(filters.len(), 2, "preserve exact OR filters");
                for event in [wrong, forged.clone(), event, forged] {
                    socket
                        .send(Message::Text(
                            RelayMessage::event(subscription_id.clone().into_owned(), event)
                                .as_json()
                                .into(),
                        ))
                        .await
                        .unwrap();
                }
                socket
                    .send(Message::Text("[\"EOSE\",\"interest\"]".into()))
                    .await
                    .unwrap();
                socket.send(Message::Text("[\"OK\",\"0000000000000000000000000000000000000000000000000000000000000000\",false,\"blocked\"]".into())).await.unwrap();
                break;
            }
        }
        loop {
            let message = socket.next().await.unwrap().unwrap();
            let Ok(text) = message.to_text() else {
                continue;
            };
            match ClientMessage::from_json(text).unwrap() {
                ClientMessage::NegOpen {
                    subscription_id,
                    initial_message,
                    ..
                } => {
                    assert_eq!(subscription_id.as_str(), "reconcile");
                    assert_eq!(initial_message.as_ref(), "6100");
                    socket
                        .send(Message::Text("[\"NEG-MSG\",\"reconcile\",\"6100\"]".into()))
                        .await
                        .unwrap();
                }
                ClientMessage::Close(id) => {
                    assert_eq!(id.as_str(), "interest");
                    break;
                }
                _ => {}
            }
        }
    });
    let mut session = RelaySession::connect(&url).await.unwrap();
    connected(&mut session).await;
    session
        .send(ClientMessage::req(
            SubscriptionId::new("interest"),
            vec![
                Filter::new().kind(Kind::TextNote),
                Filter::new().kind(Kind::Reaction),
            ],
        ))
        .await
        .unwrap();
    session
        .send(
            ClientMessage::from_json("[\"NEG-OPEN\",\"reconcile\",{\"kinds\":[1]},\"6100\"]")
                .unwrap(),
        )
        .await
        .unwrap();
    let mut event_ids = Vec::new();
    let (mut eose, mut rejected, mut neg) = (false, false, false);
    timeout(Duration::from_secs(3), async {
        while !(eose && rejected && neg) {
            let RelaySessionEvent::Message(message) = session.next().await.unwrap().unwrap() else {
                continue;
            };
            match *message {
                RelayMessage::Event { event, .. } => {
                    event_ids.push(event.id);
                }
                RelayMessage::EndOfStoredEvents(id) => {
                    eose = id.as_str() == "interest";
                }
                RelayMessage::Ok { status, .. } => rejected = !status,
                RelayMessage::NegMsg {
                    subscription_id,
                    message,
                } => neg = subscription_id.as_str() == "reconcile" && message == "6100",
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(event_ids, vec![wanted]);
    session
        .send(ClientMessage::close(SubscriptionId::new("interest")))
        .await
        .unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn reconnect_replays_only_live_interests_and_drop_closes_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        loop {
            let message = socket.next().await.unwrap().unwrap();
            if message
                .to_text()
                .ok()
                .is_some_and(|text| text.starts_with("[\"CLOSE\""))
            {
                break;
            }
        }
        socket.close(None).await.unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        loop {
            let message = socket.next().await.unwrap().unwrap();
            let Ok(text) = message.to_text() else {
                continue;
            };
            if let Ok(ClientMessage::Req {
                subscription_id, ..
            }) = ClientMessage::from_json(text)
            {
                assert_eq!(subscription_id.as_str(), "live");
                ready_tx.send(()).unwrap();
                break;
            }
        }
        while let Some(message) = socket.next().await {
            match message {
                Ok(Message::Close(_)) | Err(_) => return,
                Ok(Message::Text(text)) => panic!("unexpected replay: {text}"),
                _ => {}
            }
        }
    });
    let mut session = RelaySession::connect(&url).await.unwrap();
    connected(&mut session).await;
    for id in ["live", "closed"] {
        session
            .send(ClientMessage::req(
                SubscriptionId::new(id),
                Filter::new().kind(Kind::TextNote),
            ))
            .await
            .unwrap();
    }
    session
        .send(ClientMessage::close(SubscriptionId::new("closed")))
        .await
        .unwrap();
    connected(&mut session).await;
    timeout(Duration::from_secs(3), ready_rx)
        .await
        .unwrap()
        .unwrap();
    drop(session);
    timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}
