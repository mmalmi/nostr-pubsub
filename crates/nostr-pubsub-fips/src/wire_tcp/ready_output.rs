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
    links: HashMap<String, u64>,
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
    assert!(report.frames.is_empty());
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
    fixture.links.insert(fixture.peer.npub(), 1);
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
