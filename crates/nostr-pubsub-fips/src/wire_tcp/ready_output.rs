use super::*;
use crate::client_transport::finish_ready_turn;
use crate::{FipsPubsubClient, FipsPubsubClientOptions, TransportCommand, now_ms};
use fips_core::Config;
use fips_core::config::{IdentityConfig, PeerConfig, TransportInstances, UdpConfig};
use tokio::sync::mpsc;
use tokio::time::timeout;

struct Fixture {
    endpoints: [Arc<FipsEndpoint>; 3],
    // A separate policy context keeps its background transport loop from ever
    // polling the two drivers under test. The actual TCP/FIPS link is real UDP.
    context: FipsPubsubClient,
    sender: WireTcpDriver,
    receiver: WireTcpDriver,
    peer: PeerIdentity,
    tx: mpsc::Sender<TransportCommand>,
    rx: mpsc::Receiver<TransportCommand>,
    links: HashMap<String, crate::PeerLinkEpoch>,
}

impl Fixture {
    async fn new(budget: usize, capacity: usize) -> Self {
        let remote = endpoint(121, Vec::new()).await;
        let local = endpoint(
            122,
            vec![PeerConfig::new(
                remote.npub(),
                "udp",
                remote.bound_udp_listen_addrs().await.unwrap()[0].to_string(),
            )],
        )
        .await;
        let isolated = endpoint(123, Vec::new()).await;
        let context = FipsPubsubClient::start(
            isolated.clone(),
            FipsPubsubClientOptions {
                receive_batch_size: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let options = || WireTcpOptions {
            frame_capacity: 1024,
            peer_capacity: 1,
            inbound_peer_capacity: 0,
            queue_records_per_peer: 8,
            queue_bytes_per_peer: capacity,
            drive_io_bytes: budget,
            drive_frames: 8,
        };
        let mut sender = WireTcpDriver::bind(local.clone(), options(), 121)
            .await
            .unwrap();
        let mut receiver = WireTcpDriver::bind(remote.clone(), options(), 122)
            .await
            .unwrap();
        let peer = PeerIdentity::from_npub(remote.npub()).unwrap();
        sender.select_peers([remote.npub().to_owned()].into()).await;
        receiver
            .select_peers([local.npub().to_owned()].into())
            .await;
        timeout(Duration::from_secs(5), async {
            loop {
                sender.connect_peer(remote.npub(), now_ms()).await.unwrap();
                receiver.connect_peer(local.npub(), now_ms()).await.unwrap();
                sender.poll(now_ms()).await.unwrap();
                receiver.poll(now_ms()).await.unwrap();
                if let (Some(left), Some(right)) = (
                    sender.active.get(remote.npub()),
                    receiver.active.get(local.npub()),
                ) {
                    let (local_port, remote_port) = sender.tcp.ports(*left).unwrap();
                    if receiver.tcp.ports(*right) == Some((remote_port, local_port)) {
                        break;
                    }
                }
                tokio::select! {
                    report = sender.receive(now_ms()) => { report.unwrap(); }
                    report = receiver.receive(now_ms()) => { report.unwrap(); }
                    () = tokio::time::sleep(Duration::from_millis(1)) => {}
                }
            }
        })
        .await
        .expect("established driver fixture");
        let (tx, rx) = mpsc::channel(16);
        Self {
            endpoints: [local, remote, isolated],
            context,
            sender,
            receiver,
            peer,
            tx,
            rx,
            links: HashMap::new(),
        }
    }

    fn send(&self, value: u8) -> TransportCommand {
        TransportCommand::Send {
            peer: self.peer,
            frame: vec![value; 4],
        }
    }

    async fn turn(&mut self, first: Option<TransportCommand>) {
        finish_ready_turn(
            &self.context.inner,
            &mut self.sender,
            &mut self.rx,
            &mut self.links,
            first,
            0,
        )
        .await;
    }

    async fn receive(&mut self, count: usize) -> Vec<Vec<u8>> {
        timeout(Duration::from_secs(2), async {
            let mut frames = Vec::new();
            while frames.len() < count {
                frames.extend(
                    self.receiver
                        .receive(now_ms())
                        .await
                        .unwrap()
                        .frames
                        .into_iter()
                        .map(|(_, frame)| frame),
                );
            }
            frames
        })
        .await
        .expect("receiver gets data without any sender poll")
    }

    async fn close(self) {
        drop(self.sender);
        drop(self.receiver);
        self.context.shutdown().await;
        for endpoint in self.endpoints {
            endpoint.shutdown().await.unwrap();
        }
    }
}

async fn endpoint(secret: u8, peers: Vec<PeerConfig>) -> Arc<FipsEndpoint> {
    let mut config = Config::new();
    config.node.identity = IdentityConfig {
        nsec: Some(hex::encode([secret; 32])),
        persistent: false,
    };
    config.node.discovery.nostr.enabled = false;
    config.node.discovery.local.enabled = false;
    config.node.discovery.lan.enabled = false;
    config.transports.udp = TransportInstances::Single(UdpConfig {
        bind_addr: Some("127.0.0.1:0".into()),
        advertise_on_nostr: Some(false),
        accept_connections: Some(true),
        ..Default::default()
    });
    config.peers = peers;
    Arc::new(
        Box::pin(
            FipsEndpoint::builder()
                .config(config)
                .without_system_tun()
                .bind(),
        )
        .await
        .unwrap(),
    )
}

// Receive authenticated TCP input without accepting streams yet, so a single
// driver turn can observe a new stream with both its request and remote FIN.
async fn receive_before_drive(driver: &mut WireTcpDriver) -> usize {
    let selected = &driver.selected_peers;
    let options = &driver.options;
    let inbound = &mut driver.inbound;
    timeout(
        Duration::from_secs(5),
        driver
            .tcp
            .receive_report_filtered_datagrams(now_ms(), |peer, bytes| {
                inbound.admit(peer, bytes, selected, options, Instant::now())
            }),
    )
    .await
    .expect("authenticated TCP input")
    .unwrap()
    .datagrams
}

#[tokio::test]
async fn scheduled_service_retry_honors_a_new_provider_cooldown() {
    let mut fixture = Fixture::new(4096, 4096).await;
    let peer = fixture.peer;
    fixture.sender.abort_peer(peer).await.unwrap();
    fixture
        .sender
        .queue_frame(peer, b"pending request")
        .unwrap();
    fixture
        .sender
        .selected_peers
        .get_mut(&peer.npub())
        .unwrap()
        .next_attempt_at = Some(Instant::now());
    fixture
        .links
        .insert(peer.npub(), crate::PeerLinkEpoch::Routed);
    for _ in 0..3 {
        fixture
            .context
            .inner
            .provider_behavior
            .lock()
            .unwrap()
            .record(
                &peer.npub(),
                crate::provider_behavior::ProviderViolation::MalformedFrame,
                now_ms(),
            );
    }
    assert!(
        fixture
            .context
            .inner
            .peer_is_in_cooldown(&peer.npub(), now_ms())
    );
    // The retry timer can win selection before the queued cooldown command.
    crate::client_transport::retry_due_services(
        &fixture.context.inner,
        &mut fixture.sender,
        &mut fixture.links,
    )
    .await;
    assert_eq!(fixture.sender.connection_count(), 0);
    assert!(!fixture.sender.selected_peers.contains_key(&peer.npub()));
    assert!(!fixture.sender.queues.contains_key(&peer.npub()));
    assert!(!fixture.links.contains_key(&peer.npub()));
    assert_eq!(fixture.sender.next_service_retry_at(), None);
    fixture.close().await;
}

async fn raw_client(endpoint: &Arc<FipsEndpoint>, seed: u64) -> FipsTcpEndpoint {
    timeout(Duration::from_secs(5), async {
        while !endpoint
            .peers()
            .await
            .unwrap()
            .iter()
            .any(|peer| peer.connected)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    FipsTcpEndpoint::bind(
        endpoint.clone(),
        FIPS_NOSTR_PUBSUB_SERVICE_PORT,
        TcpConfig::default(),
        seed,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn public_client_close_reclaims_hidden_syn_and_same_turn_request_state() {
    let provider = endpoint(130, Vec::new()).await;
    let isolated = endpoint(141, Vec::new()).await;
    let context = FipsPubsubClient::start(isolated.clone(), FipsPubsubClientOptions::default())
        .await
        .unwrap();
    let address = provider.bound_udp_listen_addrs().await.unwrap()[0].to_string();
    let options = || WireTcpOptions {
        frame_capacity: 1024,
        peer_capacity: 2,
        inbound_peer_capacity: 1,
        queue_records_per_peer: 4,
        queue_bytes_per_peer: 4096,
        drive_io_bytes: 4096,
        drive_frames: 4,
    };
    let mut driver = WireTcpDriver::bind(provider.clone(), options(), 130)
        .await
        .unwrap();
    let provider_peer = PeerIdentity::from_npub(provider.npub()).unwrap();
    // A configured peer remains admitted while the public slot is recycled.
    let configured = endpoint(131, vec![PeerConfig::new(provider.npub(), "udp", &address)]).await;
    driver
        .select_peers([configured.npub().to_owned()].into())
        .await;
    let mut configured_tcp = raw_client(&configured, 131).await;

    for byte in 132..140 {
        let client = endpoint(
            byte,
            vec![PeerConfig::new(provider.npub(), "udp", &address)],
        )
        .await;
        let mut tcp = raw_client(&client, byte.into()).await;
        let id = tcp.connect(provider_peer, now_ms()).await.unwrap();
        receive_before_drive(&mut driver).await; // SYN
        timeout(Duration::from_secs(5), tcp.receive(now_ms()))
            .await
            .unwrap()
            .unwrap(); // SYN-ACK
        receive_before_drive(&mut driver).await; // ACK, not yet accepted by the driver
        assert_eq!(tcp.state(id), Some(State::Established));

        // A second connection deliberately remains hidden in SYN-RECEIVED.
        let hidden = tcp.connect(provider_peer, now_ms()).await.unwrap();
        receive_before_drive(&mut driver).await;
        assert_eq!(tcp.state(hidden), Some(State::SynSent));
        assert_eq!(driver.inbound.len(), 1);
        let frame = br#"["REQ","closed-client",{"kinds":[1]}]"#;
        let record = encode_record(frame).unwrap();
        assert_eq!(
            tcp.write(id, &record, now_ms()).await.unwrap(),
            record.len()
        );
        tcp.close(id, now_ms()).await.unwrap();
        let mut count = 0;
        while count < 2 {
            count += receive_before_drive(&mut driver).await;
        }
        let report = driver.drive_ready(now_ms()).await.unwrap();
        assert_eq!(report.frames.len(), 1);
        assert_eq!(report.newly_connected.len(), 1);
        assert_eq!(report.disconnected.len(), 1);
        assert_eq!(report.disconnected[0].npub(), client.npub());
        assert_eq!(driver.inbound.len(), 0);
        assert!(
            !driver
                .connections
                .values()
                .any(|connection| connection.peer == client.npub())
        );
        assert!(!driver.active.contains_key(client.npub()));
        assert!(!driver.inputs.contains_key(client.npub()));
        assert!(!driver.queues.contains_key(client.npub()));
        let peer = PeerIdentity::from_npub(client.npub()).unwrap();
        context.inner.handle_frame(peer, frame).await;
        assert_eq!(context.peer_subscription_count().unwrap(), 1);
        let received = context.delivery_snapshot().req_frames_received;
        crate::client_transport::process_wire_report(&context.inner, &mut driver, report).await;
        assert_eq!(context.peer_subscription_count().unwrap(), 0);
        assert_eq!(context.delivery_snapshot().req_frames_received, received);

        if byte == 132 {
            let id = configured_tcp
                .connect(provider_peer, now_ms())
                .await
                .unwrap();
            receive_before_drive(&mut driver).await;
            timeout(Duration::from_secs(5), configured_tcp.receive(now_ms()))
                .await
                .unwrap()
                .unwrap();
            driver.receive(now_ms()).await.unwrap();
            assert_eq!(configured_tcp.state(id), Some(State::Established));
        }
        assert!(driver.active.contains_key(configured.npub()));
        assert!(driver.selected_peers.contains_key(configured.npub()));
        drop(tcp);
        client.shutdown().await.unwrap();
    }
    drop(configured_tcp);
    drop(driver);
    context.shutdown().await;
    isolated.shutdown().await.unwrap();
    configured.shutdown().await.unwrap();
    provider.shutdown().await.unwrap();
}

#[tokio::test]
async fn unselected_peer_syn_is_rejected_before_tcp_admission() {
    let remote = endpoint(124, Vec::new()).await;
    let local = endpoint(
        125,
        vec![PeerConfig::new(
            remote.npub(),
            "udp",
            remote.bound_udp_listen_addrs().await.unwrap()[0].to_string(),
        )],
    )
    .await;
    let options = || WireTcpOptions {
        frame_capacity: 1024,
        peer_capacity: 1,
        inbound_peer_capacity: 0,
        queue_records_per_peer: 4,
        queue_bytes_per_peer: 4096,
        drive_io_bytes: 4096,
        drive_frames: 4,
    };
    let mut sender = WireTcpDriver::bind(local.clone(), options(), 124)
        .await
        .unwrap();
    let mut receiver = WireTcpDriver::bind(remote.clone(), options(), 125)
        .await
        .unwrap();
    sender.select_peers([remote.npub().to_owned()].into()).await;
    sender.connect_peer(remote.npub(), now_ms()).await.unwrap();
    let report = timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! {
                report = receiver.receive(now_ms()) => break report.unwrap(),
                () = tokio::time::sleep(Duration::from_millis(10)) => {
                    sender.poll(now_ms()).await.unwrap();
                }
            }
        }
    })
    .await
    .expect("unselected peer sends an authenticated TCP SYN");
    let retained = receiver.connection_count();
    drop(sender);
    drop(receiver);
    local.shutdown().await.unwrap();
    remote.shutdown().await.unwrap();
    assert!(report.tcp_datagrams > 0);
    assert_eq!(report.rejected_tcp_datagrams, report.tcp_datagrams);
    assert_eq!(retained, 0);
    assert_eq!(report.frames.len(), 0);
}

#[tokio::test]
async fn established_send_batch_flushes_without_a_sender_poll_and_counts_first_command() {
    let mut fixture = Fixture::new(4096, 4096).await;
    let first = fixture.send(0);
    for value in 1..4 {
        fixture.tx.try_send(fixture.send(value)).unwrap();
    }
    fixture.turn(Some(first)).await;
    assert_eq!(
        fixture.rx.len(),
        2,
        "first selected command consumes one batch slot"
    );
    let flushed = fixture.sender.queues.is_empty();
    if !flushed {
        fixture.close().await;
        panic!("ready bytes must leave application queue in this turn");
    }
    assert_eq!(fixture.receive(2).await, vec![vec![0; 4], vec![1; 4]]);
    fixture.turn(None).await;
    assert_eq!(fixture.receive(2).await, vec![vec![2; 4], vec![3; 4]]);
    // Report handling also queues directly (new-connection REQs and WANT retries).
    fixture
        .sender
        .queue_frame(fixture.peer, b"report reply")
        .unwrap();
    fixture.turn(None).await;
    assert_eq!(fixture.receive(1).await, vec![b"report reply".to_vec()]);
    fixture.close().await;
}

#[tokio::test]
async fn ready_output_preserves_byte_budget_and_stops_on_tcp_backpressure() {
    let mut fixture = Fixture::new(8, 64).await;
    for value in 0..2 {
        fixture
            .sender
            .queue_frame(fixture.peer, &[value; 4])
            .unwrap();
    }
    // An errored drive may have consumed an unknown amount before returning.
    // The transport loop conservatively exhausts its post-handler allowance.
    finish_ready_turn(
        &fixture.context.inner,
        &mut fixture.sender,
        &mut fixture.rx,
        &mut fixture.links,
        None,
        usize::MAX,
    )
    .await;
    assert_eq!(fixture.sender.queues[&fixture.peer.npub()].bytes, 16);
    let written = fixture.sender.flush_queues(now_ms(), 0).await.unwrap();
    assert_eq!(written, 8);
    // A receive report and its response flush share one byte budget.
    finish_ready_turn(
        &fixture.context.inner,
        &mut fixture.sender,
        &mut fixture.rx,
        &mut fixture.links,
        None,
        written,
    )
    .await;
    assert_eq!(fixture.sender.queues[&fixture.peer.npub()].bytes, 8);
    fixture.turn(None).await;
    assert!(fixture.sender.queues.is_empty());
    // Withhold sender receive/poll, hence its TCP ACK processing. Fill the
    // 64-byte send window with eight 8-byte records, then leave two queued.
    for value in 2..10 {
        fixture.tx.try_send(fixture.send(value)).unwrap();
        fixture.turn(None).await;
    }
    let pending = fixture.sender.queues[&fixture.peer.npub()].bytes;
    assert_eq!(pending, 16);
    fixture.turn(None).await;
    assert_eq!(
        fixture.sender.queues[&fixture.peer.npub()].bytes,
        pending,
        "zero-byte acceptance returns without draining or spinning"
    );
    // Processing the peer's ACKs is a real readiness wake, and permits progress
    // without a sender timer poll. The receiver alone drains its bounded reads.
    for _ in 0..8 {
        fixture.receiver.poll(now_ms()).await.unwrap();
        let _ = timeout(
            Duration::from_millis(10),
            fixture.receiver.receive(now_ms()),
        )
        .await;
    }
    timeout(Duration::from_secs(2), async {
        while fixture
            .sender
            .queues
            .get(&fixture.peer.npub())
            .is_some_and(|queue| queue.bytes == pending)
        {
            fixture.sender.receive(now_ms()).await.unwrap();
        }
    })
    .await
    .expect("ACK-driven ready IO resumes the queued write");
    fixture.close().await;
}

#[tokio::test]
async fn ready_command_batch_preserves_send_cooldown_order() {
    let mut fixture = Fixture::new(4096, 4096).await;
    fixture
        .links
        .insert(fixture.peer.npub(), crate::PeerLinkEpoch::Direct(1));
    fixture
        .tx
        .try_send(TransportCommand::Cooldown { peer: fixture.peer })
        .unwrap();
    fixture.turn(Some(fixture.send(1))).await;
    assert!(fixture.sender.queues.is_empty());
    assert!(fixture.sender.active.is_empty());
    assert!(fixture.links.is_empty());
    let errors = fixture
        .context
        .inner
        .transport_errors
        .load(crate::Ordering::Relaxed);
    fixture.turn(Some(fixture.send(2))).await;
    assert_eq!(
        fixture
            .context
            .inner
            .transport_errors
            .load(crate::Ordering::Relaxed),
        errors + 1
    );
    assert!(
        fixture.sender.queues.is_empty(),
        "send after deselection stays rejected"
    );
    fixture.close().await;
}

#[tokio::test]
async fn ready_batches_deliver_repeated_bursts_without_timer_polls() {
    let mut fixture = Fixture::new(4096, 4096).await;
    for batch in 0..32_u8 {
        let first = fixture.send(batch * 2);
        fixture.tx.try_send(fixture.send(batch * 2 + 1)).unwrap();
        fixture.turn(Some(first)).await;
        assert!(fixture.sender.queues.is_empty());
        assert_eq!(
            fixture.receive(2).await,
            vec![vec![batch * 2; 4], vec![batch * 2 + 1; 4]]
        );
    }
    assert_eq!(
        fixture
            .context
            .inner
            .transport_errors
            .load(crate::Ordering::Relaxed),
        0
    );
    fixture.close().await;
}

#[tokio::test]
async fn discovered_inbound_peer_keeps_its_pending_reply_when_selected() {
    let remote = endpoint(148, Vec::new()).await;
    let local = endpoint(
        149,
        vec![PeerConfig::new(
            remote.npub(),
            "udp",
            remote.bound_udp_listen_addrs().await.unwrap()[0].to_string(),
        )],
    )
    .await;
    let options = || WireTcpOptions {
        frame_capacity: 1024,
        peer_capacity: 2,
        inbound_peer_capacity: 1,
        queue_records_per_peer: 4,
        queue_bytes_per_peer: 4096,
        drive_io_bytes: 4096,
        drive_frames: 4,
    };
    let mut sender = WireTcpDriver::bind(local.clone(), options(), 148)
        .await
        .unwrap();
    let mut receiver = WireTcpDriver::bind(remote.clone(), options(), 149)
        .await
        .unwrap();
    sender.select_peers([remote.npub().to_owned()].into()).await;
    timeout(Duration::from_secs(5), async {
        loop {
            sender.connect_peer(remote.npub(), now_ms()).await.unwrap();
            sender.poll(now_ms()).await.unwrap();
            receiver.poll(now_ms()).await.unwrap();
            if sender.active.contains_key(remote.npub())
                && receiver.active.contains_key(local.npub())
            {
                break;
            }
            tokio::select! {
                r = sender.receive(now_ms()) => { r.unwrap(); }
                r = receiver.receive(now_ms()) => { r.unwrap(); }
                () = tokio::time::sleep(Duration::from_millis(1)) => {}
            }
        }
    })
    .await
    .expect("inbound peer establishes its request stream");
    let peer = PeerIdentity::from_npub(local.npub()).unwrap();
    receiver
        .queue_frame(peer, b"pending historical reply")
        .unwrap();
    receiver
        .select_peers([local.npub().to_owned()].into())
        .await;
    // Selection may add local subscriptions, but a reply already owed to this
    // authenticated peer must survive discovery of its advertised service.
    receiver.connect_peer(local.npub(), now_ms()).await.unwrap();
    let delivered = timeout(Duration::from_secs(2), async {
        loop {
            sender.poll(now_ms()).await.unwrap();
            receiver.poll(now_ms()).await.unwrap();
            tokio::select! {
                r = sender.receive(now_ms()) => {
                    if r.unwrap().frames.iter().any(|(_, frame)| frame == b"pending historical reply") { break; }
                }
                r = receiver.receive(now_ms()) => { r.unwrap(); }
                () = tokio::time::sleep(Duration::from_millis(1)) => {}
            }
        }
    }).await;
    drop(sender);
    drop(receiver);
    local.shutdown().await.unwrap();
    remote.shutdown().await.unwrap();
    assert!(
        delivered.is_ok(),
        "discovering a peer must preserve its queued history reply"
    );
}
