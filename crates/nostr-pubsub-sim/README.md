# nostr-pubsub-sim

A bounded, adversarial discrete-event simulator for decentralized Nostr
pubsub. Scheduling and observable timestamps use a virtual millisecond clock
with no sleeps; timestamps with equal deadlines retain deterministic FIFO
order. A scheduled-action budget stops scenarios that would otherwise amplify
without bound, while arrived protocol messages are counted separately.

The simulator compares three peer-selection modes:

- `neutral`: no learned peer behavior or shared reputation;
- `local-behavior`: production mesh scores learned from malformed messages and
  protocol outcomes;
- `shared-reputation`: local behavior plus signed, transported machine ratings
  evaluated by `nostr-social-graph`, while reserving unknown-peer exploration.

## Production-shaped traffic

Each scenario creates real signed Nostr events, verifies them, and matches them
with production Nostr filters. Honest peers are divided across eight
representative subscription shapes:

1. author feed: kind 1 plus an exact author;
2. hashtag topic: kind 1 plus a NIP-12 `t` tag;
3. Hashtree update: kind 30064 plus author, `d`, and `l=hashtree`;
4. targeted approval/rating: kind 7368 plus recipient `p`;
5. Iris Drive broad root: kind 30078 plus `d`, intentionally without an author
   constraint;
6. FIPS advert: kind 37195 plus application `d`;
7. FIPS paid offer: the production kind-wide kind 37196 discovery shape;
8. NIP-34 repository announcement: kind 30617 plus author and repository `d`.

Subscriptions traverse the production FIPS pubsub `REQ`/`CLOSE` codec,
adapter, and bounded subscription store. Each directed link has one combined
subscription: its ordinary profile filters and, in shared-reputation mode, its
machine-rating filters occupy the same `REQ` and store entry. Event-class
admission still decides which part applies. Publications traverse the
production `InvWantMesh`, inventory/want/frame codec, routing, deduplication,
retry, and filter-based subscriber selection. Supernodes use the same paths
with a larger fanout, larger connection capacity, an all-kinds mesh, and a
subscribe-all filter. Their role is hidden simulator ground truth; it is never
encoded in a FIPS advert or exposed to candidate selection.

Ordinary peers keep only their organic profile filters: attackers do not add a
filter to a victim to manufacture interest. Where attacker identity does not
itself make a match impossible, signed adversarial rounds alternate between
events that naturally match an existing profile and semantic near misses with a
different tag, recipient, or application identifier. Broad production filters
may still admit such variants, which remains visible in the results;
exact-author filters reject attacker-signed substitutions at the subscription
boundary.

## Admission and adversaries

In `shared-reputation`, admission is deliberately split by event class without
assuming that users maintain follow or mute lists:

- author, hashtag, Hashtree, and repository traffic is admitted by the
  recipient's production Nostr subscription filters. It does not require a
  positive social edge. The simulator does not construct or mutate human
  follow/mute graphs;
- machine admission covers targeted ratings, FIPS adverts, paid offers, and
  peer-rating control traffic. Production `PeerRatingPublisher`, signed kind
  7368 events, FIPS subscriptions, `InvWantMesh`, and `PeerReputation` carry
  positive admission and removal transitions. One subject is admitted, removed,
  and re-admitted by three transported ratings one virtual second apart.
  A sampled machine-WoT lane may publish at most two positive service
  endorsements per selected observer, and selects one in sixteen observers.
  One evidence-qualified observer is admitted as a globally bounded fallback
  only when that sampling produces no endorsement at all. The AuthorFeed
  workload includes three signed service samples so the production threshold
  is exercised without lowering it.
  Endorsement requires three first-accepted verified legitimate service frames
  on that directed link with no invalid or unserved evidence; rating traffic
  cannot endorse itself, and negative evidence wins.
  Declared-rater forgeries are transported to and rejected by the production
  ingester. A separate valid, self-signed compromised-trusted-rater probe
  measures the removal power intentionally granted to an authorized rater.
  Another properly signed peer defects only after verified useful service made
  it reachable to one receiver. It poisons two unknown neighbors, then sends
  five malformed production wire frames across that exact relationship. The
  receiver publishes machine-derived negative evidence, revokes the peer,
  requires both poisoned targets to recover from removed to unknown, and sends
  a later rating by the revoked key through another relay to prove production
  event admission drops it before ingestion.
  Four valid ratings from distinct graph-unconnected keys are a separate,
  bounded retained-state and CPU-pressure control. They are intentionally inert
  in the graph and do not claim to test social-graph spam suppression. No
  topology neighbor is pretrusted: a rating-author subscription is installed
  only after verified service and a local-root positive graph projection, then
  removed when the local root revokes that relation;
- application admission checks broad Iris Drive roots against authors learned
  by applying production Nostr filter matching to established signed history.

Adversarial load includes Sybil and blackhole peers, malformed wire frames,
syntactically valid fake inventories that do not yield events, signed spam for
all eight subscription classes, subscription floods and limit violations,
forged machine ratings, an authorized poisoned rating, and adversarial generic
discovery candidates. Seeded packet loss, bounded link churn, supernode outages, delayed
transfer retries, and eventual disrupted-transfer delivery run on the virtual
clock. Persistent attacker identities repeat after reputation can propagate;
separately keyed fresh Sybils first appear later as a cold-start control. Every
identity lane is reported both across all matching traffic and for the three
machine-admitted subscription classes, so learned removal can be separated
from filter mismatch. The poisoned-rating probe targets an honest non-neighbor
when possible, so it measures trust-anchor risk without manufacturing a
workload edge; it is still accounted as spam. Historically legitimate frames
from the explicit post-service defector are classified separately by author
even when an honest peer relays them. The simulator demonstrates these risks,
not a defense against a compromised trust anchor.

The virtual packet-loss injector operates at the simulator's record-transport
boundary: a dropped record represents a locally observed failed stream/link
attempt because the simulator does not reproduce TCP segment retransmission.
Its route-local disruption mark applies only to that request attempt and a
retry restores normal unserved-provider accountability. Production adapters
must emit this evidence only for confirmed local stream/link failure, never
from peer claims or an inferred missing application response.

The combined profile-and-rating `REQ`/`CLOSE` control frames also traverse the
virtual scheduler and production FIPS adapter. Legitimate controls retry after
loss or a down link within the configured retry bound; malformed, unauthorized,
and flood traffic does not. Reconnect replay begins only after the reconnect
subscription arrives and is accepted. A retry counts as recovered only after
its intended store state change and any reconnect replay complete. Attempted
and received control traffic contributes to the same node/link service
accounting, and control loss contributes to the packet-drop KPI.

## Topologies and role-blind endpoint selection

`peer-mesh` builds bounded cohort rings, same-interest shortcuts, cross-cohort
links, and attacker exposure. `hybrid-supernodes` assigns some honest nodes
larger hidden resource limits and builds their simulator-configured backbone.
Normal peers select from generic endpoint IDs—all honest endpoints plus the
configured number of adversarial endpoints—without receiving role or capacity:

- `bootstrap`: seeded generic bootstrap candidates;
- `interest-affinity`: candidates ranked by matching self-claimed subscription
  cohort;
- `exploration`: seeded selection across all generic candidates;
- `mixed`: one generic bootstrap link plus exploration and claimed-cohort
  affinity.

The connection attempt order never reads `NodeRole::Supernode` or the hidden
high-capacity set. A candidate can accept more links only because the simulated
endpoint enforces a larger local connection limit. High-capacity selection and
coverage are classified afterward from ground truth; they are measurements,
not routing inputs or peer claims.

Hybrid scenarios also give one persistent attacker and one fresh-Sybil control
a bounded ingress link to an honest supernode. Those two workload-pressure
links traverse the normal subscription and inv/want paths but are not counted
as normal-peer endpoint selections.

Interest-affinity discovery is **not** verified
`nostr-social-graph` evidence. Cohort IDs are self-claimed, attackers may claim
ordinary cohorts, so this strategy is not mistaken for a trust oracle. Machine
reputation is used separately by event and mesh admission.

Runtime rediscovery revisits only the normal peer's outbound endpoint
selections; the hidden simulator backbone remains topology setup. On the virtual
clock, each peer removes an unavailable endpoint, a machine-policy rejection,
confident local invalid/unserved behavior, or a connection whose received
production subscription state has no overlap with signed established-history
probes for the peer's primary interest. A permanent disconnect clears that
peer's production wire-adapter subscription state. Replacement walks a seeded
per-peer permutation of generic endpoint IDs and uses the normal machine
admission policy, FIPS REQ subscription exchange, reconnect replay, and
inv/want mesh paths.

Each peer may replace at most two links and inspect at most 64 candidates per
sweep. The cursor scans each endpoint at most once, while retained state stores
only the cursor and endpoints retired from observed service. Ground-truth roles
are consulted only after admission to classify KPIs and to make a selected
adversarial endpoint behave adversarially. A scheduled recovery cannot resurrect
a connection that rediscovery has removed.

## KPIs

The CSV report has one row per topology and peer-selection mode. It covers:

- delivery ratio per subscription class, worst-class delivery, delivered-sample
  latency percentiles and maximum, sample count, and undelivered events;
- aggregate expected/delivered signed-spam raw counts and outcome suppression,
  delivery basis points for each of the eight subscription classes,
  persistent-versus-fresh-Sybil raw counts and outcome suppression, and the
  same identity split restricted to machine-admitted classes. Here,
  outcome suppression means expected matching deliveries that did not arrive,
  while the separate filter and policy-drop counters identify why. Those
  filter/graph/machine/application drops, legitimate drops, forged-rating
  publication/evaluation/rejection, valid poisoned-rating
  publication/ingestion/removal, and
  separate uninterested legitimate and spam deliveries;
- production subscription-store decisions for every active signed-spam link
  toward an ordinary peer, including opportunities, suppressions, and basis
  points separately for all eight subscription classes;
- inventory/want/frame counts, separate data-plane and FIPS control-plane wire
  bytes, legitimate/adversarial workload-provenance bytes, legitimate byte
  share, protocol messages and bytes per interested delivery, queue depth,
  processed actions versus arrived messages, and exact byte-conservation
  checks;
- loss, churn, retry inventories, eventual delivery after disrupted transfers,
  blackhole drops, and sends made while a candidate remained locally unknown;
- machine rating publication/transport/ingestion, admission/removal
  transitions, unserved-inventory-only quiet-blackhole removals, deliberate
  poisoning removals versus honest-observer false positives, same-subject
  admit/remove/readmit completion, removal latency, machine trust-edge counts,
  signed machine graph updates, bounded positive-endorsement state, rating
  protocol messages/bytes, retained ratings/raters/roots, and separately
  classified service-admitted-rater poison, revocation, target-recovery, and
  post-revocation policy-drop outcomes. The graph-unconnected valid-rating
  batch is reported only as bounded retained-state/CPU pressure, not as a graph
  spam-defense result;
- scheduled subscription messages and bytes, reliable-control resend and
  recovery counts (`subscription_retries` and
  `subscription_retry_recoveries`), rejection, bounded eviction, and
  close/reopen behavior;
- after-the-fact high-capacity selection precision and peer coverage,
  adversarial-candidate selections, peers without a high-capacity selection,
  supernode maximum/mean load, and load Gini concentration;
- runtime rediscovery sweeps, candidate attempts, removed/replacement links,
  observed adversarial and unavailable removals, after-the-fact high-capacity
  replacements, bounded retained state, and replacement-subscription control
  messages/bytes.

### Honest-node resource measurement

Resource distributions are reported separately for all honest nodes, ordinary
honest peers, and honest supernodes. Use p50 to understand normal cost, but use
p95, p99, and maximum as the optimization and denial-of-service signals:

- bandwidth is the exact encoded pubsub payload bytes sent and received by each
  simulated node, including inv/want data and FIPS subscription/control
  payloads. `combined_bytes` is endpoint I/O (`sent + received`), so one payload
  crossing a link appears at both endpoints; it is not unique network traffic.
  These numbers exclude FIPS stream framing, transport encryption, IP headers,
  and substrate retransmission. Measure or estimate those separately. Useful
  payload credits only successful remote legitimate deliveries to interested
  ordinary peers. Attacker-sent adversarial bytes versus honest adversarial
  endpoint I/O gives the victim bandwidth-amplification ratio;
- CPU is a deterministic, per-node vector of production-path work: encoded and
  decoded codec bytes, signature checks, filter queries and candidates, mesh
  candidates, graph queries, rating events considered, and reputation rebuild
  entries. It separately counts checks skipped by verified-event APIs and
  reports the exact no-fast-path signature-check counterfactual, so the p95 CPU
  optimization gate does not depend on wall-clock noise. Filter candidates are
  a conservative upper bound because matching may short-circuit. The simulator
  deliberately assigns no universal weights. Calibrate these primitives with
  production-code microbenchmarks on each target class of hardware, then
  validate matched release scenarios with process CPU counters and a profiler.
  The whole simulator's CPU time includes scheduler and shared simulation
  overhead and must not be divided by its simulated node count;
- retained memory reports simultaneous high-water and final values for exact
  encoded content: cached event payloads, canonical subscription `REQ` state,
  local filters and events, and queued wire payloads. State-entry counts cover
  mesh routes and deduplication plus subscriptions, filters, ratings, raters,
  and trust roots. Encoded content is a lower bound, not Rust heap usage: struct,
  hash-table, allocation, and allocator overhead are excluded. Calibrate entry
  footprints and validate RSS or heap profiles in an isolated production-shaped
  node process. Do not divide the multi-node simulator's RSS by node count.

`virtual_ms` is protocol time for delivery latency, churn, outages, retry, and
reputation convergence. The virtual clock has no sleeps and says nothing about
CPU duration or throughput of the simulator itself.

### Incentive and bilateral-risk scenarios

Every verified delivery trail is also priced under five optional service-layer
models: direct verified Cashu, bounded offline peer credit, a fixed prepaid
Cashu lease, an incremental Cashu Spilman channel, and accepted-mint batching.
These plans do not change FMP forwarding or make `fips-core` payment-aware.

The ordering of payment and useful work matters:

- accepted-mint batching is post-delivery settlement for small verifiable work,
  such as a requested event or hash-valid block. It has provider unpaid
  exposure, not buyer prepaid exposure. Residual value below the Cashu threshold
  becomes bounded peer credit; reaching the pair cap requires Cashu, and further
  work is denied if payment is unavailable. Defaulted credit remains explicit
  provider loss rather than being reported as Cashu-settled;
- a fixed prepaid lease transfers its whole service-window amount before an
  unknown provider performs. Provider failure therefore becomes buyer
  counterparty loss and failed work is not counted as honest earned service.
  Paid value is conserved as verified use, retained unused service credit, or
  loss; actual later demand is never used to shrink the up-front exposure;
- an incremental Spilman channel locks capacity in a 2-of-2 Cashu output, but
  the provider can claim only the latest cooperatively signed balance. Unused
  capacity remains sender value. A signed increment is already seller-claimable;
  failed cooperative close extends the buyer's liquidity lock to the refund
  timelock rather than making that signed work unpaid. A strategic payer can
  fund a channel and withhold the next signature, but modeled provider loss is
  capped at one update. The model separately records signed balance, returned
  capacity, lock duration, opening/closing fees, and refund failure instead of
  treating all locked capacity as a payment to the seller;
- peer credit and post-delivery direct Cashu put a bounded useful-service risk
  on the provider instead of prepayment risk on the buyer.

Peer credit here is a bilateral, unsecured, non-transferable IOU capped for one
pair. A closed-loop Cashu token is different: it is a prefunded, transferable
liability of its issuer and is useful to any connected peer that accepts that
mint, subject to issuer and acceptance limits. Accepted-mint batching models
private mint compatibility and reciprocal netting; it does not yet model issuer
solvency, withdrawal liquidity, or a transferable multihop credit market.

`honest_earned_settled_by_deadline` is service-level settlement: it includes
bounded peer credit an honest provider accepted, as well as reciprocal service
and external Cashu. The separate peer-credit accepted, outstanding, and
per-pair peak fields prevent accepted credit from being mistaken for Cashu.

The verified-one-shot selector retains the 5% payment-byte preference and does
not open a channel or choose fixed prepayment. The untrusted-streaming selector
first minimizes the worse of buyer prepayment and provider grace exposure, then
counterparty loss, locked liquidity, fees, and wire bytes. It may therefore select
incremental Spilman despite payment bytes above 5%; the byte target is not a
reason to transfer a much larger prepaid amount to an unknown seller.

CSV output includes both recommended strategies and these modeled KPIs:
`buyer_prepaid_exposure_sat`, `buyer_counterparty_loss_sat`,
`provider_unpaid_exposure_sat`, `provider_default_loss_sat`,
`peer_credit_accepted_sat`, `peer_credit_outstanding_sat`,
`max_pair_peer_credit_sat`,
`locked_capital_sat_ms`, peak channel capacity, aggregate capacity/signed/unused/
returned value, opening and closing fees, and refund failures/loss. It also
emits fixed-prepayment paid/used/unused-credit/loss conservation beside the
selected streaming plan. The `channel_value_conserved` column requires capacity
to equal signed plus unused value and unused value to equal returned plus lost
value. Payment message sizes,
fees, failures, and liquidity time are modeled inputs; calibrate them against
real CDK/`cdk-spilman` runs before production capacity decisions.

The optional Cashu integration gate checks those modeled assumptions against
`cashu-service` and CDK code. It consumes an actual production-path verified
delivery record, enforces the peer-credit cap, rejects unobserved service and
globally replayed backing, converts credit through a route-bound withdrawable
reserve, and delivers genuine proofs exactly once. A real local withdrawable
mint can then pay fake Lightning while a closed-loop mint cannot; forged proofs,
payout redirection, and online double-spend replay are rejected, with reserve
conservation checked throughout:

```sh
cargo test -p nostr-pubsub-sim --features cashu-integration \
  --test cashu_integration -- --nocapture
```

This gate uses no real funds or external network. It covers one-shot Cashu
proof validity and reserve conservation, not Spilman open/update/close/refund;
the latter remains covered by the upstream `cdk-spilman` real-mint suites.

### KPI priority and optimization targets

Optimize in this order. A cheaper system is not an improvement if it censors
honest traffic or stops delivering it:

1. protocol/security correctness: exact accounting, no forged-rating bypass,
   and zero honest-observer false removals;
2. legitimate availability: at least 95% aggregate and 90% worst subscription
   cohort delivery in the release matrix;
3. adversarial tail cost: honest-peer and honest-supernode p99/maximum CPU-work,
   retained memory, and bandwidth, not only averages;
4. attack leverage: victim bandwidth amplification and persistent
   machine-admitted spam suppression, with at least 50% suppression;
5. useful-delivery efficiency: bytes and calibrated CPU per interested remote
   delivery;
6. tail latency, disrupted-transfer recovery, and machine-removal convergence;
7. supernode load concentration and dependence on any one topology;
8. median clean-load cost and simulator runtime.

For an optimization candidate, compare identical seeds and topologies against a
recorded shared-reputation baseline. The working resource goal is at least a
20% reduction in either the dominant honest-peer p95 CPU-work component or
bytes per useful delivery, without more than a 10% regression in calibrated
process CPU/RSS or delivery latency. A 10x longer or heavier spam workload
should add no more than 10% to quiescent production pubsub retained state after
expiry; application-owned local history is reported separately and is not
mistaken for expiring mesh state. Keep ordinary-peer cached event payloads
within 16 MiB and supernode payloads within 256 MiB. Correctness and delivery
thresholds above are hard constraints, not terms in a weighted score.

Traffic ledgers retain legitimate/adversarial workload provenance in both
directions for every directed link and aggregate it by peer, supernode, and
attacker role. Provenance describes the generated workload, not the carrier:
attacker bootstrap and flood controls are adversarial, while a legitimate event
remains legitimate if an attacker relays it. The report exposes independent
data-plus-control, provenance, sent-link, and sent-role byte totals and requires
all four accounting views to conserve `total_protocol_bytes` exactly.
Only a successful remote legitimate delivery to an interested ordinary peer
credits its final directed hop; local delivery, supernode receipt, spam, and
uninterested receipt do not receive delivery credit. Credits are also
aggregated by carrier role, so hybrid scenarios can prove that supernodes
complete interested deliveries rather than merely absorb control or spam. The
hybrid release gate uses the stricter `supernode_third_party_interested_*`
KPIs: the final-hop carrier must be a hidden-role supernode and the event's
original publisher must be a different node. A supernode delivering its own
forced workload therefore cannot establish supernode usefulness.

The kind-wide kind 37196 paid-offer subscription is intentionally broad, so
filter matching alone cannot distinguish an honest offer from a Sybil-signed
offer. Its per-class result represents an economic-admission gap, not a solved
incentive. The service planner compares bounded local payment strategies, but
global Cashu/Lightning liquidity routing, prices, utilities, and strategic
equilibrium remain outside this simulation. The link/role ledgers and delivery
credits preserve the underlying service facts so richer strategies can be
compared later without replacing the network model.

## Running 1,000 nodes

The default is 1,000 nodes, including 200 attackers, and runs both topologies
under all three peer-selection modes:

```sh
cargo run --release -p nostr-pubsub-sim
```

The ignored release gate runs a deterministic 18-case matrix: three seeds,
both topologies, and all three peer-selection modes, with 1,000 nodes per case.
It requires at least 95% aggregate and 90% worst-cohort legitimate delivery,
at least 50% peer-mesh and 50% mixed-hybrid signed-spam outcome suppression,
at least 50% suppression in the persistent machine-admitted identity lane,
zero honest-observer machine-removal false positives, exact accounting
conservation, and no shared-policy delivery regression against the neutral and
local controls.
Fresh machine-admitted Sybils are compared with the lossy local control rather
than treated as learned identities.

```sh
cargo test --release -p nostr-pubsub-sim --test release_gate -- \
  --ignored --nocapture --test-threads=1
```

An explicit adversarial configuration is useful for reproducible comparisons:

```sh
cargo run --release -p nostr-pubsub-sim -- \
  --nodes 1000 \
  --attackers 200 \
  --topology all \
  --mode all \
  --discovery mixed \
  --adversarial-discovery-candidates 8 \
  --fanout 6 \
  --unknown-reserve 1 \
  --max-hops 16 \
  --fake-inventories-per-attack-link 6 \
  --signed-spam-rounds 8 \
  --loss-bps 200 \
  --churn-bps 300 \
  --retry-ms 80 \
  --max-retries 3 \
  --action-budget 10000000
```

To compare generic endpoint-selection assumptions, repeat a fixed seed and all other
arguments with `--topology supernodes` and each of `--discovery bootstrap`,
`interest-affinity`, `exploration`, and `mixed`. The legacy `social-graph`
spelling remains accepted as a CLI alias.
