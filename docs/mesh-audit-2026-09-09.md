# Ad hoc mesh audit — 2026-09-09

The native stack can carry events and blobs across authenticated peers without
public Nostr relays. Full product-level ad hoc meshing is still incomplete:
bootstrap, browser transports, application subscriptions, blob forwarding, and
machine reputation have different coverage. Passing a pubsub simulation does
not demonstrate that every product can discover a fresh peer or recover a
real network partition.

This audit inspected nostr-pubsub, FIPS, fips-tcp, Hashtree, Iris Chat Rust,
Iris Drive, Nostr VPN, and the mesh-facing nostr-social-graph adapters. Coverage
below describes tested source. Publishing and downstream adoption are separate
acceptance steps; a passing library test does not identify an installed product's
dependency version.

## Published versions and adoption

| Component | Published version | Scope |
| --- | --- | --- |
| FIPS core and endpoint | 0.4.78 | Registry libraries with scoped source tags; no separate FIPS application release implied |
| FIPS TCP | Rust 0.2.2, endpoint 0.2.14, TypeScript 0.2.2 | The TypeScript package is an immutable GitHub release archive, not an npm publication |
| nostr-pubsub-fips | 0.5.1 | Registry adapter with routed peers, bounded turnover and durable retry recovery |
| Hashtree | CLI 0.2.146, embedded 0.2.89, FIPS transport 0.4.17 | Published native artifacts and downstream package adoption |
| Iris Chat Rust | v2026.9.9 | Native artifacts published; TestFlight delivery succeeded and App Store submission is awaiting review |
| Iris Drive | 0.1.36 | Native packages published; internal TestFlight 0.1.36 (1037) delivered |
| Nostr VPN | Existing release lane | Adoption of the final FIPS dependency tuple is owned and verified by that lane |

Hashtree's five platform builds and artifact startup checks passed. Its published
installer checks passed on Windows and both Linux architectures. Hosted macOS
verification stopped at an anonymous GitHub API rate limit before product
commands; the same verifier passed locally against the immutable macOS archive,
checking the installed binary bytes and storage/helper behavior. These distinct
outcomes are retained rather than treating the hosted failure as a passing job.

Drive's release source is `77f0c624`, with an annotated `v0.1.36` tag on its
canonical Hashtree repository. A fresh anonymous clone verified every retained
reference and all 18,658 reachable objects. The native artifacts have recorded
build origins and verified input equivalence through the final release source;
later harness-only commits do not imply that every binary was rebuilt. Its
standard publisher completed Hashtree distribution and Zapstore publication.
Independent public downloads matched all nine verified artifacts, totaling
582,605,729 bytes; versioned and latest manifests match the release source.

## Current product coverage

| Component | Working path | Remaining boundary |
| --- | --- | --- |
| nostr-pubsub | Rust/TypeScript bounded inventory/want state machines; real Rust FIPS/TCP subscriptions, replay, reconnect recovery, and routed service identities across intermediates with no pubsub service | Known service identities need an available physical route. The high-level FIPS client and scale simulator use different integration paths. Default physical-peer selection is bounded but not quality-ranked. |
| FIPS | Authenticated multihop routing; native UDP/TCP and platform-specific link transports; LAN discovery and optional same-host rendezvous | Some configured bootstrap paths still use public infrastructure. Native routing tests do not establish browser or hardware-radio readiness. |
| Hashtree | Real FIPS blob transport, central hash verification, explicit mesh forwarding, exact hop decrement and exhaustion | Forwarding must actually be configured. The TypeScript worker preserves hop budgets but has no equivalent of Rust's generic MeshForwardingRoute. The old hashtree-sim mesh model is retired. |
| Iris Chat Rust | Known-contact signed events use standard pubsub and the existing decryption/receipt handlers, independently of the sibling listener; durable retry beyond the replay cache; authenticated receipts stop mesh retries; Hashtree attachment routes | Fresh identities still need discovery and a physical path. Static WSS seeds remain a default bootstrap dependency. Nearby BLE/LAN remains a separate bounded packet/receipt path. The graph ranks profile search, not mesh peers. |
| Iris Drive | FIPS UDP, pubsub and same-host rendezvous enabled by default; live routed roster of authorized clients; shared BlobRouter with FIPS forwarding and roster authorization | Default WSS seeds remain a bootstrap dependency. Platform restrictions disable some LAN/WebRTC paths. Pubsub has no social-graph admission policy. |
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

A simple integration contract is one general trustworthiness prior for initial
peer preference, adjusted by local observations of latency, failures and invalid
data. Preserve unknown-peer exploration and hard resource bounds. Keep authority
to submit ratings about other identities explicit: the poisoning tests show why
a history of useful service cannot establish that authority by itself. This
needs a permission decision, without introducing three separate reputation
scores. Applying that uniform contract across products remains follow-up work.

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

## Initial audit reproduction and validation

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

These initial simulation results do not measure process CPU throughput, browser
firewall/NAT traversal, or wireless hardware. Native process and idle-resource
checks added subsequently are described below.

## Highest-value remaining acceptance work

1. Extend the native known-identity partition/rejoin tests below to fresh
   discovery, browser connectivity and hardware links. Keep VPN adoption with
   its product release lane. Assert delivered messages and verified blobs,
   not just connected-peer counts.
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
5. Define and test cancellation of the lower-level `FipsInvWantStream` and
   `FipsInvWantTcpDriver` when a custom asynchronous event policy suspends.
   Code inspection shows records are removed from decoder storage before that
   await; cancelling the enclosing call could discard the local record/action
   buffer. This is an inferred API hazard, not reproduced product loss, and
   was not the cause of the Chat outage. No affected production Hashtree caller
   was found in this audit.
6. Automate the native CPU and bandwidth gate across upstream releases, then
   extend it with browser background/resume workloads and longer reconnect/churn
   runs. Compare Windows sampled CPU against cumulative process time. Measure
   mobile data and screen-off energy on hardware before setting battery claims;
   current simulator and short native idle checks do not cover that cost.

Products pin published dependencies. Local fixes in capability repositories
require a separate tested dependency rollout before installed products benefit.

## Native routed-service slice

The bounded acceptance target is signed control events and hash-verified
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
The adapter suite passes 46 tests, including the same three-node exchange over
real loopback UDP with Nostr, LAN, and local discovery disabled. The unchanged simulator's production-scale
release matrix and retained-state gates pass again.

A single general reputation prior is enough for the current scope. Measured
latency, failed requests, and invalid payloads are local operational evidence,
not additional global character scores. Authority to influence another peer's
reputation remains explicit: FIPS accepts ratings from self or configured
trusted authors. Good service alone must not silently grant that authority.
The poisoning tests show why that boundary matters even if connection selection
uses one score. No new transport/blob/rater score dimensions are introduced.

## Outage recovery and resource regressions

Testing actual applications exposed failures that the initial simulations did
not cover:

- A retained FIPS session could keep retrying an empty published route after
  its intermediate peer restarted. Refreshing that session's route before
  pumping queued data restored a persistent 350-KiB TCP transfer in about
  6.3 seconds; the previous implementation delivered no data within 60 seconds.
- Losing many small TCP writes in one flight recovered only the oldest segment
  per timeout. The timeout kept backing off for each subsequent missing segment,
  even while acknowledgements advanced. A deterministic regression drops
  44 separately written 101-byte segments, including sequence-number wraparound.
  Recovery now repairs at most one old-flight segment per advancing ACK,
  retaining timeout backoff and per-segment retry limits. Duplicate/invalid ACKs,
  a closed receive window and later normal traffic do not trigger that repair.
- A durable outbox retry could find an event's ID in the seen cache after its
  payload had been evicted, preventing retransmission. An explicit local retry
  now restores that payload within the existing bounds. Incoming gossip still
  deduplicates normally.
- An oversized record prefix could occupy a decoder without becoming ready for
  rejection. Four prefix bytes now suffice to reject it and close that stream;
  already decoded frames from healthy peers survive the same driver turn.
- Chat generated Nearby traffic when the actual Nearby service was disabled.
  Gating publication on the running service eliminated those unused datagrams.
  This removed about 2.7 MB from the failing 70-message diagnostic scenario;
  it was separate from the TCP delivery failure.

The real WebSocket regression warms a connection, stops the sole intermediate
router, queues 70 signed events against a 64-event replay window, and restarts
the router. The old TCP delivered 0/70 within the 40-second recovery deadline;
the repaired implementation passes. Neither endpoint nor its subscription is
restarted. Separate cases cover a single publication without application retry,
tree routing with one pubsub slot, and reply-learned routing with the normal
64-peer budget and an uninterested transit peer.

At Chat source `a4cafb1bb382593c9886d0a4314cf80292ef7850` (`v2026.9.9`),
the integration test recovers all 70 encrypted messages in 11.127 seconds after
the intermediate router restarts, decrypts their original plaintexts and
processes authenticated Seen receipts. The test uses published registry
dependencies without local patches or temporary diagnostics. It then requires
at least 15 seconds with no new EVENT, INV, WANT or REQ frames, no unresolved
mesh outbox work and at most one liveness wake. Optional relay outbox records
remain available. Across the actual 19.486-second quiet observation, the two
transit links carried 36,406 bytes combined (1,868 B/s).

A separate earlier debug candidate, with temporary diagnostics still present,
measured 1.90% of one CPU core for both Chat cores plus the router over a
10-second sample. That CPU sample is not a measurement of the released native
app or mobile battery use.

The native iris-stack product fixture carries signed events and hash-verified
192-KiB blobs between Chat and Drive through a Hashtree router that has no
pubsub service and is outside the Drive application ACL. It checks partition
recovery, a blob created during the partition, provider replacement and hop
exhaustion. Its resource gate samples both before the outage and after recovery:

| Resource gate | Default bound | What it catches |
| --- | ---: | --- |
| Idle process CPU, each fixture | 5% of one core over 15 seconds | Busy loops, repeated polling or background work that does not settle |
| Idle transit TX + RX, both links combined | 4 KiB/s over 15 seconds | Retry floods or repeated control/publication traffic |
| Chat after authenticated receipts | Zero new EVENT/INV/WANT/REQ in 15 seconds | Durable outbox retries that fail to stop |
| Pubsub tenfold spam scenario | At most 10% quiescent retained-state growth | Unbounded retained protocol state under hostile input |

Three final runs using immutable Chat `a4cafb1b`, Drive `05751a82` and Hashtree
`70519509` fixtures all passed. Median initial signed-event delivery was 92 ms,
initial blob retrieval 227 ms, recovery after the intermediate restarted
13.587 seconds, and fresh post-recovery blob retrieval 134 ms. Combined transit
traffic was 2,564.5–2,652.6 B/s before the outage and 2,927.7–3,107.7 B/s after
recovery. All 18 per-process CPU observations were available and measured
1.00–1.53% of one core. Every checkpoint retained the required 1/2/1 direct-peer
topology and zero relays. These short local fixture measurements establish the
bounded known-identity slice, not browser discovery or day-long mobile use.

The [hosted released-product lab](https://github.com/irislib/iris-stack/actions/runs/34403954478)
also passed at Iris Stack `c706147`, installing the same public source pins with
the standard release-profile builders. All three external-product tests passed.
The optimized relayless fixture delivered four signed events and two verified
blobs with the required 1/2/1 peers and zero public relays. Initial event delivery
took 77 ms, the first blob 139 ms, router-restart recovery 13.155 seconds and the
fresh blob 49 ms. Idle CPU was 0.33–0.40% per process; combined transit traffic
was 2,660.5 B/s before the partition and 2,920.8 B/s after recovery. Each idle
window lasted 15 seconds. These optimized Ubuntu results complement the local
fixture runs; they do not measure a mobile application or hardware energy use.

CPU sampling uses process CPU time, with Linux clock ticks or macOS subsecond
accounting. Missing or corrupt measurements are reported explicitly, never as
zero usage. CPU budgets are enforced when cumulative readings are available;
unsupported sampling or missing tools is reported as unavailable. Other
invocation, read or parse failures fail the gate. Both traffic samples require
two live transit peers, so a disconnected router cannot pass by reporting zero
traffic. The idle budgets are generous
regression limits, not performance targets. They do not replace the simulator's
active-load work/byte counters or an eventual hardware energy test.

The standard `iris-stack/scripts/product-lab.sh` command runs these resource
checks with the external product fixtures. Its relevant-path push/PR workflow
and reusable workflow run that command. Ordinary `cargo test --all-targets`
skips the ignored external-product tests, and upstream Chat, Drive and Hashtree
releases do not automatically invoke the cross-product workflow. This release
coordinates the final pinned matrix explicitly; continuous enforcement across
every upstream release remains a separate integration step.

Reviewing Drive's existing desktop CPU gates found measurement false passes:
a required process could disappear or restart after an early observation;
POSIX could clamp a reset CPU counter to zero; Windows could count absent or
null performance data as zero. Commit `5c87c75a` requires the original required
process set throughout the sample and rejects missing readings. Cumulative
POSIX counters also reject resets; Windows uses formatted percentage readings.
Controlled inputs through the actual script entry points reproduced six POSIX
false passes across Linux/macOS and four under Windows PowerShell 5.1. The fixed
scripts reject all those cases, still accept stable idle and still reject excess
CPU. The original CPU thresholds, sampling windows and optional-role semantics
remain unchanged. These are sampler regressions, separate from running native
applications through their idle windows.

The same review found duplicated iOS host and Android samplers could also
accept a vanished or replaced app, reset CPU counters, and (on Android) missing
or nonadvancing device uptime. Drive commit `9f42889d` reuses the hardened POSIX
sampler for the iOS host path and checks Android process/counter continuity.
Actual script entry points reproduce 11 former false passes; all 26 focused
POSIX/mobile scenarios now pass their expected assertions. Reuse removes 99
production lines, with the original budgets and windows preserved.

An Android native run with optimized Rust code and the UI-test shell then
passed the corrected gate after 90 seconds
of settling and 60 seconds of sampling: 2.03% mean CPU, 2.78% peak, against the
5% mean limit. The initialized, authorized profile had freshly active FIPS before
the sample and two connected peers afterward. All nine Android UI tests and
actual linking/file synchronization passed beforehand. This uses an emulator;
it is not a physical-device energy measurement. An earlier iOS zero-CPU sample
is excluded from active-mesh evidence because its app remained on setup and did
not establish fresh FIPS activity. A stale simulator test override selected an
old app-group path after reinstall. Clearing that override made the unchanged
optimized app open the authorized profile and start FIPS in 1.174 seconds.
The launch harness now clears an inherited override unless the caller explicitly
sets one. The corrected optimized iOS simulator run then measured 2.28% average
app CPU and 4.73% peak across 12 intervals, under the 5% mean limit, with the
authorized profile and fresh FIPS activity checked throughout. This is host
process CPU for a simulator, not a physical-device battery result.

Linux's whole-second `ps` CPU accounting also proved too coarse near small
budgets: 3.54 CPU seconds in a 60-second window (5.9%) could be reported as
3 seconds (5%) and pass. Commit `8f31e717` reads the kernel's per-process user
and system CPU ticks instead, rejects unreadable or corrupt counters, and
preserves process selection and all budgets. Causal checks verify 0.2% usage,
reject the former 5.9% false pass, and exercise malformed counters. All 36
executed sampler/harness cases pass; the Windows-only class is separately
covered by the earlier PowerShell 5.1 proof. The earlier optimized Linux GUI
sample reported no CPU-time increase at whole-second resolution; it must not
be read as proof of exactly zero CPU. The same optimized Linux binaries then
passed a fresh 30-second settle/60-second sample with kernel counters: the GUI
averaged 0.10% (0.20% peak) and its daemon 0.18% (0.40% peak), below the
unchanged 5% and 10% limits. This was an authorized single-device profile with
public discovery disabled; connected-mesh measurements remain separate.

The optimized Windows app and daemon also passed the stock 30-second settle
and 60-second sample with 12 intervals. Its formatted Windows performance
counter reported 0% for both processes. That integer reading is not proof of
exactly zero or subpercent CPU usage, and the sampler does not yet compare
against cumulative kernel/user CPU time. UI Automation navigation and an
isolated Cloud Files registration passed with both processes running as the
same ordinary user. Visible-control bounds and foreground identity were checked,
but the screenshots retained blank content and a stale desktop clock; visual
verification is unavailable from those captures.

Two further Drive harness regressions make the native checks trustworthy on
shared machines. Windows GUI selection and cleanup now match the test's exact
executable, preserving another running copy of the app. Cross-machine daemon
checks transfer the invoking checkout's sampler, filter by the test's isolated
configuration, and sample hosts sequentially. They previously loaded whatever
sampler happened to be in the remote default checkout; the Windows filter could
also include an unrelated daemon. Causal mocked-process and emitted-command
tests cover these fixes. No CPU budget or sampling window was relaxed.

A real Windows–Linux run then exposed another false-pass path in the remote
runner: PowerShell processed the input as separate statements, skipped compound
setup, and could continue after an error or silently accept incomplete syntax.
Drive commit `77f0c624` sends one ASCII invocation that decodes and parses the
entire UTF-8 script before executing it. The payload remains on stdin, avoiding
command-line size limits. Ten actual PowerShell 5.1 cases pass, including
128-KiB input, Unicode, malformed syntax and stopping before a later statement
after a terminating error. The production encoder emits the exact tested bytes;
portable helper/store checks pass ten tests with the native PowerShell method
skipped there because it was exercised separately on Windows.

The Windows–Linux functional run reports roughly 13–20 seconds for ordinary
file-change checks. These are full test completion times: three-second polling,
two stable matching snapshots, remote status/list calls and projection checks.
They are not isolated transfer latency. Updates are event-driven rather than
waiting on a corresponding periodic reconciliation timer. Root metadata can
arrive before its asynchronous block download, so a local-only list may briefly
report a missing chunk. The harness retries and requires complete matching
snapshots before accepting convergence. Attributing latency needs mutation,
root-receipt, block-completion and projection timestamps; no additional failure
was established from those transient reads.

Explicit Drive profiles also needed application isolation. Windows now scopes
single-instance coordination, its local pipe and Cloud Files identity to the
selected configuration while preserving the default profile's names. macOS maps
each custom domain to its own configuration and storage instance; it no longer
removes unrelated domains. Focused native-language regressions cover concurrent
profiles and the mapping contract. The signed Mac VM run passed device linking
and subsequent file-byte handoff, then an optional Finder-folder assertion hit
the system's older-provider version guard. Backup was not reached in that run.
No candidate domain was created or existing provider ownership replaced. Finder
behavior remains unverified by this run; the standard signed/notarized artifact
gate is separate and does not enable that optional assertion.

The optimized Mac app subsequently passed its stock 60-second settle and
60-second idle sample: app mean 0.20%/peak 0.59% against the 5% mean limit,
daemon mean 0.56%/peak 0.79% against 10%. All 12 intervals retained the original
process sets and an authorized profile with fresh FIPS. This uses the supported
development configuration on a task-only copy: its file-backed executable
sections match the signed originals, while signing metadata differs and sandbox
restrictions are inactive. The notarized artifacts remain untouched. Earlier
signed temporary-profile attempts failed sandbox access; the development
fixture also needed its existing external-provider-runtime setting to avoid
probing protected group storage before daemon startup. Those attempts are
excluded from idle evidence.

The standard Windows–Linux daemon matrix passed all functional phases, including
restarts, concurrent edits, many small files and a large file, then sampled each
host sequentially after 180 seconds of settling. Across 12 five-second intervals,
Linux averaged 1.61% CPU (2.0% peak) and Windows 1.92% (6% peak), below the 10%
mean limit. Linux uses cumulative kernel ticks; Windows uses integer formatted
percentages. This matrix uses normal discovery/relay settings, separate from the
zero-public-relay fixture proof. Its child exited successfully, but the outer
supervisor caught and reaped a remaining local runner process group. Independent
readback confirmed no owned remote processes, mounts or folders and unchanged
existing Windows process identities. Focused watchdog tests, an actual Windows
SSH start/stop and the complete orchestration with mocked endpoints all passed
with no surviving process groups. They did not reproduce the original cleanup
failure or identify its cause. The original supervisor exit 125 remains
recorded separately from the successful product and CPU checks; no speculative
cleanup change was made.

Drive's live authorized-peer roster had a separate growth failure: initial
binding truncated it to the pubsub capacity, but later refreshes did not.
A shared helper now deduplicates, orders and bounds both paths consistently.
The production capacity remains 64 with at most 63 routed identities. An actual
endpoint/client regression with 70 authorized identities fails before the fix
and passes afterward; smaller explicitly configured budgets remain respected.

A controlled comparison using identical fixture binaries found no material
idle saving from reducing the pubsub peer budget from 64 to 1: roughly
2.6–2.8 KB/s at the intermediate node and 0.47–1.13% CPU per process in both
configurations. The production peer budget remains unchanged. A special retry
or transit-exclusion mechanism would add complexity without a demonstrated
benefit in this measurement.

The remaining idle rate deserves further profiling: sustained continuously,
2.6–2.8 KB/s would total roughly 225–242 MB/day of combined transit traffic.
That is an extrapolation from short local samples, not a day-long measurement
or a claim about mobile data use. Lowering this settled traffic
while preserving the outage-recovery gates is a useful next performance target.
The measurements do not yet attribute those bytes to individual FIPS control
messages.

Testing the real Drive CLI exposed an additional relayless startup failure:
an explicitly empty relay list still entered the raw relay subscription path
and terminated the daemon. Embedded browser storage also appended default
resolver relays to an empty application relay list. Drive commit `9522c87b`
honors the empty list in both paths, keeps direct FIPS processing active and
skips a relay-only pending-approval retry worker when no relays are configured.
The disabled relay receiver remains pending, avoiding an immediate-ready idle
loop. Startup diagnostics now preserve the full error chain.

A new production-daemon regression creates two independently keyed profiles
and admits the second through signed offline roster operations. With relay,
Blossom, WSS seed, LAN and rendezvous discovery all disabled, static loopback
UDP peers start successfully and exchange signed roots and verified file bytes
in both directions. The test passes in 45.77 seconds. Sequential 15-second
idle samples measure 1.05% and 1.04% of one CPU core, with peaks of 1.84% and
1.82%; the existing daemon limit is 10%. These are native debug CLI processes,
not mobile application or battery measurements. Native package rebuilds and
platform gates remain separate acceptance steps.
