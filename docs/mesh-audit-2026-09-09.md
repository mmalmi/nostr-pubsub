# Ad hoc mesh audit — 2026-09-09

The native stack can carry events and blobs across authenticated peers without
public Nostr relays. Full product-level ad hoc meshing is still incomplete:
bootstrap, browser transports, application subscriptions, blob forwarding, and
machine reputation have different coverage. Passing a pubsub simulation does
not demonstrate that every product can discover a fresh peer or recover a
real network partition.

This audit inspected nostr-pubsub, FIPS, fips-tcp, Hashtree, Iris Chat Rust,
Iris Drive, Nostr VPN, and the mesh-facing nostr-social-graph adapters. Work is
local source integration; it is not a release or downstream dependency rollout.

## Current product coverage

| Component | Working path | Remaining boundary |
| --- | --- | --- |
| nostr-pubsub | Rust/TypeScript bounded inventory/want state machines; real Rust FIPS/TCP subscriptions, replay, reconnect recovery, and default multihop endpoint adverts with no relay sockets | The high-level FIPS client and the scale simulator use different integration paths. Ordinary events require matching subscriptions at intermediate pubsub peers; endpoint adverts subscribe by default. |
| FIPS | Authenticated multihop routing; native UDP/TCP and platform-specific link transports; LAN discovery and optional same-host rendezvous | Some configured bootstrap paths still use public infrastructure. Native routing tests do not establish browser or hardware-radio readiness. |
| Hashtree | Real FIPS blob transport, central hash verification, explicit mesh forwarding, exact hop decrement and exhaustion | Forwarding must actually be configured. The TypeScript worker preserves hop budgets but has no equivalent of Rust's generic MeshForwardingRoute. The old hashtree-sim mesh model is retired. |
| Iris Chat Rust | Known sibling sync over FIPS/TCP; recent-peer hints; optional Nearby BLE/LAN; opt-in same-host discovered Hashtree blobs and Blossom | Standard pubsub starts with device sync, not as a universal app event bus. Nearby messages use a separate bounded packet/receipt path. Empty relay lists incorrectly gated sibling sync before this audit's fix. First contact still needs an available relay/Nearby path; static WSS seeds remain a bootstrap dependency. The graph ranks profile search, not mesh peers. |
| Iris Drive | FIPS UDP, pubsub and same-host rendezvous enabled by default; shared BlobRouter with FIPS forwarding and roster authorization | Default WSS seeds remain a bootstrap dependency. Platform restrictions disable some LAN/WebRTC paths. Pubsub has no social-graph admission policy. |
| Nostr VPN | Relayless client mode; persistent pubsub subscriptions; FipsPubsubPolicy evaluates verified events before caching/gossip; roster ownership protects tunnel traffic separately from transit | Default relay bridge mode differs from explicit relayless mode. LAN participation is configurable. Native product tests exist, but a new cross-product/browser end-to-end matrix was not run here. |

Relevant product ownership: Chat `core/src/core/device_sync/runtime.rs`,
`core/src/core/fips_nearby.rs`, `core/src/core/profile_search.rs`; Drive
`crates/iris-drive-core/src/fips_sync.rs` and
`crates/iris-drive-core/src/fips_sync/{nostr_runtime,blob_runtime,settings_runtime}.rs`; VPN
`crates/nostr-vpn-cli/src/control_pubsub_runtime.rs` and its endpoint/config
modules. Exact paths and local commits are reported with the corresponding
changes.

## Pubsub scale results

The existing optimized gate passed all three ignored tests: an 18-case matrix
(three seeds × peer/hybrid topology × neutral/local/shared policy), four
discovery strategies, and the baseline/tenfold-spam retained-state comparison.
The routing/discovery matrices use 1,000 nodes, including 200 attackers, real signed Nostr
events, production filters/codecs/InvWantMesh, and a deterministic virtual clock.
The release configuration uses fanout 6, one unknown-peer slot, 16 hops, 2% loss,
3% churn, and bounded retries. It models connected links; it does not perform
NAT traversal or run 1,000 product processes.

The simulator baseline was nostr-pubsub commit `19f9877` (core/simulator
0.1.13). The local adapter fix does not change those simulation state machines.

| Shared reputation, release matrix | Peer mesh | Hybrid supernodes |
| --- | ---: | ---: |
| Legitimate delivery | 99.90–100% | 99.94–100% |
| Worst subscription cohort | 99–100% | 99.87–100% |
| Signed-spam suppression | 54.33–58.41% | 53.84–53.87% |
| Disrupted-transfer recovery metric | 72.48–74.64% | 97.27–99.55% |
| False removals from honest observations | 0 | 0 |

The incomplete disrupted-transfer recovery metric remains a useful target even
though aggregate delivery passes: multiple paths and redundant publications
can mask individual failed transfers.

The separate 64-node, 16-attacker retained-state comparison increased signed
spam events from 64 to 640 and fake inventories
from 888 to 8,448. After expiry, maximum retained protocol content stayed at
10,874 bytes and maximum state entries stayed at 129. These are encoded-content
and entry counters, not allocator memory or process RSS.

A separate identical-seed 18-case fanout sweep is preserved in
[mesh-audit-2026-09-09-fanout.csv](mesh-audit-2026-09-09-fanout.csv).
Shared-policy results:

| Topology / fanout | Delivery | Worst cohort | p95 delivery, virtual ms | Total protocol bytes |
| --- | ---: | ---: | ---: | ---: |
| Peer / 4 | 93.20% | 89.00% | 135 | 19,522,445 |
| Peer / 6 | 100% | 100% | 78 | 21,212,289 |
| Peer / 8 | 100% | 100% | 66 | 21,961,569 |
| Hybrid / 4 | 99.81% | 99.61% | 126 | 51,727,768 |
| Hybrid / 6 | 100% | 100% | 120 | 52,806,299 |
| Hybrid / 8 | 100% | 100% | 118 | 53,188,725 |

Fanout 4 fails the delivery floor in the peer topology. Fanout 6 saves only
3.4% of peer-mesh bytes versus 8 while increasing p95 latency by 18.2% in this
seed. No production fanout default was changed. Latency percentiles include
delivered events only and must always be read beside availability.

## What nostr-social-graph contributes, and what it costs

`nostr-pubsub-social-graph::PeerReputation` projects signed kind-7368 machine
ratings in the `fips.peer` scope. It checks signature/rater binding, freshness,
scope, latest-event ordering, entry bounds, and trust reachability. The default
policy keeps unknown identities eligible; it does not require a human follow
list. The core mesh reserves exploration capacity.

The simulator demonstrates materially better spam resistance, but not a blanket
resource saving. At fanout 6, shared versus neutral policy increases total
protocol bytes by 1.34× in the peer mesh and 2.38× in the hybrid. Ordinary-peer
p95 combined bytes rise from 56,612 to 61,252 and from 77,822 to 214,300,
respectively. These totals include rating/control traffic and changed discovery
behavior; they do not isolate the cost of graph queries. Target subscription
refresh, rating dissemination, and rediscovery separately before tuning policy.

The zero false-removal counter is explicitly limited to honest observations.
The gate also requires a compromised trusted-rater probe to cause 1–2 wrongful
removals, and a service-admitted malicious rater to remove two initially unknown
targets before revocation restores both. Signature verification stops forged
identity; it does not establish the truth of an authorized rating. Positive
service history and authority to judge unrelated peers need distinct policy
consideration. Fresh Sybil identities and broad paid-offer filters remain open
admission problems.

Integration is uneven. VPN applies the shared graph-backed event policy. Chat
uses the graph for profile search; Drive does not currently use it for meshing.
FIPS itself accepts signed ratings through its external event-ingestion API and
uses self/configured trusted-signer scores, retaining the newest score per peer,
without depending on nostr-social-graph. A trusted signer may attest a tagged
rater different from itself; this is a different authority model from the
pubsub adapter's signer/rater binding. FIPS's query lookback is not a stored
score expiration policy.
The lower-level FIPS inv/want stream accepts both peer and event policies;
the high-level FipsPubsubClient exposes event-policy admission and separate
local provider-abuse cooldowns. These are not one uniform graph-backed peer
selection system across products.

## Reproduced pubsub fix

An endpoint with more connected peers than `max_connected_peers` previously
made every peer snapshot fail. It could prevent client startup and disable
subscription creation and transport synchronization as a mesh grew.

The adapter now keeps a stable bounded subset, preserving the application's
other FIPS links. A real three-endpoint regression sets pubsub capacity to one
while the center has two authenticated FIPS links. Before the fix startup
fails; afterward the selected stream delivers a signed application event and
both underlying links stay connected.

Turnover testing exposed two further state leaks: queued records and old peer
subscriptions could keep capacity occupied after the chosen peer changed.
Permanent deselection now releases those records, pending observations and
subscriptions. The driver rejects stale outbound requests and unselected
inbound streams, while same-identity session replacement preserves restartable
queued records. Startup selection precedes initial REQs, and failed connection
attempts remain retryable. A capacity-one regression replaces a live peer and
verifies delivery in both directions without recreating the application
subscription; a driver regression covers partially queued records and stale
send/connect attempts.

This is an availability fix, not a new peer-selection strategy: the subset is
still ordered by identity. Quality-aware selection and exploration at this
high-level boundary remain future work.

## FIPS routing and rating freshness

The production-backed `fips-sim` comparison ran 48 nodes, 144 edges, seed 42,
96 round-trip probes per phase, 600 background packets, and four 128-KiB
raw-datagram transfers. The impaired phase introduced 6% blackhole peers,
6% flaky peers with 30% drop, 4% churned nodes, and 6% down links.

| Routing / phase | Successful probes | p95 successful probe, ms | Transfer setups | Chunks delivered after setup |
| --- | ---: | ---: | ---: | ---: |
| Tree / baseline | 92/96 (95.8%) | 2,346 | 4/4 | 96.3% |
| Tree / impaired | 71/96 (74.0%) | 1,588 | 2/4 | 93.4% |
| Reply-learned / baseline | 91/96 (94.8%) | 1,076 | 4/4 | 97.7% |
| Reply-learned / impaired | 85/96 (88.5%) | 1,148 | 3/4 | 96.6% |

Reply-learned routing improved impaired probe availability by 14.6 percentage
points in this run. Eleven of 96 probes still failed. This single-seed result
does not justify changing defaults. Percentiles omit failures, chunk rates are
conditional on successful setup, and these datagrams are not reliable TCP
streams. Reproduce in FIPS:

```sh
cargo run -p fips-sim --example production_mesh -- \
  --compare --nodes 48 --route-probes 96 --stream-probes 4 \
  --stream-bytes 131072 --background-packets 600 --summary-only
```

A separate Web-of-Trust pubsub model reached 127 peer adverts from two initial
contacts and rejected 40/40 untrusted spam events in a 128-node graph. Its
inventory/want accounting used 34,347,376 bytes versus 127,188,624 for its
full-flood comparator, about 73% less. This is a control-plane model; only its
six health probes used production FIPS endpoints. It is not a 128-process
product or production pubsub benchmark.

FIPS commit `6ccdfd42` rejects signed rating facts with publication or observation
timestamps beyond the existing 60-second skew tolerance. Previously, an accepted
future observation could pin the newest-per-peer ranking. The regression first
reproduced acceptance of a timestamp 120 seconds ahead; the fix passed 40
discovery runtime tests and six open-discovery admission tests. Historical
imports remain valid. FIPS/TCP verification additionally passed 32 Rust tests
and three Rust/TypeScript loss/reversal/duplication interoperability tests.

## Local implementation commits

- nostr-pubsub `326b208`: bounded working peer subset instead of whole-client
  failure above capacity.
- nostr-pubsub `10a9b11`: free deselected-peer state, reject stale work, and
  preserve same-peer reconnect recovery while allowing peer replacement.
- FIPS `6ccdfd42`: reject future-dated signed rating facts.
- Hashtree `d79876be`: let independent coalesced readers recover when the
  shared request owner is cancelled or its earlier deadline expires.
- Hashtree `394cd1b8`: prefer the configured store for inbound blobs while
  retaining slow-store hedging through existing bounded blocking-read work.
- Iris Drive `fb55a91c`: retain live pubsub subscriptions across peer changes,
  using the transport adapter's existing reconnect/replay behavior.
- Iris Chat Rust `e1ab0a72`: allow known sibling transport with an empty relay
  list and skip creating an empty relay announcement provider.

## Hashtree forwarding and product simplification

Hashtree's regression composes real `BlobRouter` and `MeshForwardingRoute`
instances. Cancelling a shared upstream request owner previously failed all
16 independent readers waiting behind it. The fix delivers to 16/16, using one
replacement upstream request: two attempts total including the cancelled one.
A separate deadline case preserves each reader's own time budget.

New deterministic topology probes cover 1, 2, 4, 8, and 10 forwarding hops,
exact hop-budget exhaustion, a cyclic diamond with a corrupt provider, and an
intermediate cache after the original provider departs. A 4-KiB cold blob costs
4,139 blob-protocol bytes per hop (41,390 across ten hops). The corruption
fallback uses four attempts and 12,460 bytes; retrieval from the intermediate
cache after provider departure uses one hop and 4,139 bytes. These are modeled
carrier bytes around production route implementations, excluding real
FIPS/TCP framing and link overhead.

The existing three-node daemon test separately traverses actual FIPS transport:
HTL 2 arrives as 2 → 1 → 0, while insufficient budgets stop forwarding. It
passed again with the cancellation fix. The 22-test FIPS blob transport suite
also passed, including provider replacement, hedging, corrupt data rejection,
stream cancellation, and concurrent transfers larger than 1 MiB. The routing
suite passed 20 tests and strict lint validation.

The probes exposed another cost: warm inbound reads could repeat all cold-read
network bytes because adaptive remote-route ranking beat a previous local
miss, even after caches were populated. A private CLI adapter now prefers the
configured store through the existing route-preference API. Blocking store
reads use the existing bounded storage queue, allowing hedge timers to run;
no public APIs, dependencies or queues were added. In the actual three-node
FIPS regression, repeat-read blob-wire bytes fall from 8,278 to 4,139 (50%),
and repeat traffic from the intermediate node to the original provider falls
from 4,139 to zero. The requester still traverses one link to the intermediate
cache; this is not a claim of zero traffic for every warm-read path.

Twenty-three CLI tests matching FIPS passed, including two new blocked-store
hedge/deadline cases, plus four existing storage queue/admission tests.
Strict production-library lint passed. The broader all-target CLI lint remains
blocked by pre-existing vendored heed, storage-test and pool-migration warnings;
it is not reported as passing.
Started blocking reads may outlive their callers, but retain the existing
admission permits. Generic router cache semantics are unchanged; the browser
worker already explicitly prefers its IDB route. The configured store may
itself fall back to a remote backend, so preserving hedging matters.

Drive's simplification removes 31 production lines, two endpoint snapshots per
second per idle application, and subscription recreation on topology changes.
The adapter already detects authenticated session changes and replays active
REQs. Five focused integration tests passed, including an update published
before any adjacency and delivered to a late peer without republishing.

Chat's regression first failed because an empty relay list prevented Alice's
sibling endpoint from starting. The fix removes that gate and three net
production lines. The focused device-sync suite passed 32 tests. Ten paired
randomized runs then passed 20/20 real WebSocket scenarios, covering both
configured-relay and empty-relay modes; each sends two messages and rejects
an outsider attempting to use the sibling listener. The test harness now
services both peers' application inboxes, so real anti-entropy requests are
handled; the earlier one-sided harness could produce false convergence
timeouts. This proves known-sibling delivery, not universal relayless first
contact or adoption of the standard pubsub bus for all chat traffic.

## Reproduction and validation

From the nostr-pubsub repository:

```sh
cargo test --release -p nostr-pubsub-sim --test release_gate -- --ignored --nocapture --test-threads=1
cargo test --release -p nostr-pubsub-fips
cargo test --release -p nostr-pubsub-social-graph -p nostr-pubsub-sim --lib
cargo test --release -p nostr-pubsub-sim --test source_size
cargo clippy --release -p nostr-pubsub-fips --all-targets --no-deps -- -D warnings
```

The FIPS adapter passed 43 tests, including no-relay multihop adverts, policy
rejection before forwarding, late connection, forced reconnect, and the new
capacity and bidirectional turnover regressions. Simulator/library tests passed 124 cases; the social
adapter passed 19. Source-size and strict adapter lint checks passed.

Build `nostr-pubsub-sim` in release mode, then repeat this command with fanout
4, 6, and 8 for the CSV sweep. The seed is explicit:

```sh
cargo run --release -p nostr-pubsub-sim -- \
  --nodes 1000 --attackers 200 --fanout 6 --max-hops 16 \
  --seed 5642820479741875522 --topology all --mode all --discovery mixed \
  --fake-inventories-per-attack-link 6 --signed-spam-rounds 8 \
  --loss-bps 200 --churn-bps 300 --retry-ms 80 --max-retries 3 \
  --action-budget 10000000
```

No CPU-throughput, browser firewall/NAT, wireless hardware, or complete
cross-product relay-removal benchmark is claimed by these results.

## Highest-value remaining acceptance work

1. Run actual Chat, Drive/Hashtree and VPN processes through bootstrap, removal
   of all relay connectivity, anchor death, partition/rejoin, and fresh versus
   cached discovery. Assert delivered messages and verified blobs, not just
   connected-peer counts.
2. Apply a consistent transport-peer selection contract across high-level
   clients, retaining unknown-peer exploration. Keep application message
   authorization and the authority to rate other machines explicit; successful
   blob service alone is not evidence that every third-party rating is true.
3. Profile and reduce hybrid rating/control dissemination and repeated
   rediscovery while retaining the measured delivery and poisoning/revocation
   gates. The present hybrid shared-policy bandwidth cost is substantial.
4. Exercise real browser-to-native forwarding, relay removal, and restrictive
   connectivity. Native Rust routing and TypeScript codec parity do not by
   themselves establish an operational browser ad hoc mesh.

Products pin published dependencies. Local fixes in capability repositories
require a separate tested dependency rollout before installed products benefit.

## Native routed-service slice

The next bounded acceptance target is signed control events and hash-verified
blobs between known native application identities through an uninterested FIPS
router, including automatic recovery after that router restarts. It does not
assume that every physical neighbour subscribes to the same application topics.

The high-level pubsub client previously selected only authenticated direct
neighbours. An application can now supply a bounded `routed_peers` roster and
replace it while subscriptions remain alive. Ordinary FIPS routing carries the
end-to-end authenticated TCP stream; intermediate nodes need no pubsub service.
A three-node regression verifies bidirectional signed events, a one-stream
capacity, no direct endpoint shortcut, invalid roster updates leaving the
current roster intact, and subscription recovery after roster removal/rejoin.
The adapter suite passes 45 tests; the unchanged simulator's production-scale
release matrix and retained-state gates pass again.

A single general reputation prior is enough for the current scope. Measured
latency, failed requests, and invalid payloads are local operational evidence,
not additional global character scores. Authority to influence another peer's
reputation remains explicit: FIPS accepts ratings from self or configured
trusted authors. Good service alone must not silently grant that authority.
The poisoning tests show why that boundary matters even if connection selection
uses one score. No new transport/blob/rater score dimensions are introduced.
