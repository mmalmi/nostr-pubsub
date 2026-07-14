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
  and `nostr-social-graph` policies, while reserving unknown-peer exploration.

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
adapter, and bounded subscription store. Publications traverse the production
`InvWantMesh`, inventory/want/frame codec, routing, deduplication, retry, and
filter-based subscriber selection. Supernodes use the same paths with a larger
fanout, larger connection capacity, an all-kinds mesh, and a subscribe-all
filter.

Ordinary peers keep only their organic profile filters: attackers do not add a
filter to a victim to manufacture interest. Where attacker identity does not
itself make a match impossible, signed adversarial rounds alternate between
events that naturally match an existing profile and semantic near misses with a
different tag, recipient, or application identifier. Broad production filters
may still admit such variants, which remains visible in the results;
exact-author filters reject attacker-signed substitutions at the subscription
boundary.

## Admission and adversaries

In `shared-reputation`, admission is deliberately split by event class:

- human admission uses `nostr-social-graph` follow distance for author,
  hashtag, Hashtree, and repository traffic; contact-list additions, removals,
  stale updates, and mute-list removal are exercised with signed Nostr kind 3
  and kind 10000 events. These updates enter the production local
  `SocialGraph` API; they are not claimed as transported graph-sync traffic;
- machine admission covers targeted ratings, FIPS adverts, paid offers, and
  peer-rating control traffic. Production `PeerRatingPublisher`, signed kind
  7368 events, FIPS subscriptions, `InvWantMesh`, and `PeerReputation` carry
  positive admission and removal transitions. One subject is admitted, removed,
  and re-admitted by three transported ratings one virtual second apart.
  Declared-rater forgeries are transported to and rejected by the production
  ingester. A separate valid, self-signed compromised-trusted-rater probe
  measures the removal power intentionally granted to an authorized rater.
  Valid untrusted ratings can be structurally ingested for transitive graph use,
  but only trusted/reachable raters change the local projection. The complete
  human contact graph and machine-rater choices use independent hash domains,
  exclude overlap per honest node, and are counted separately: a follow does
  not authorize a machine rating, and machine trust does not create a follow;
- application admission checks broad Iris Drive roots against authors learned
  by applying production Nostr filter matching to established signed history.

Adversarial load includes Sybil and blackhole peers, malformed wire frames,
syntactically valid fake inventories that do not yield events, signed spam for
all eight subscription classes, subscription floods and limit violations,
forged machine ratings, an authorized poisoned rating, and false supernode
candidates. Seeded packet loss, bounded link churn, supernode outages, delayed
transfer retries, and eventual disrupted-transfer delivery run on the virtual
clock. Persistent attacker identities repeat after reputation can propagate;
separately keyed fresh Sybils first appear later as a cold-start control. Every
identity lane is reported both across all matching traffic and for the three
machine-admitted subscription classes, so learned removal cannot be confused
with filter mismatch or human-follow policy. The poisoned-rating probe
targets an honest non-neighbor when possible, so it measures trust-anchor risk
without manufacturing an ordinary data-path failure; it is still accounted as
spam. The simulator demonstrates this risk, not a defense against a compromised
trust anchor.

Ordinary and rating `REQ`/`CLOSE` control frames also traverse the virtual
scheduler and production FIPS adapter. Legitimate controls retry after loss or a
down link within the configured retry bound; malformed, unauthorized, and flood
traffic does not. Reconnect replay begins only after the reconnect subscription
arrives and is accepted. A retry counts as recovered only after its intended
store state change and any reconnect replay complete. Attempted and received
control traffic contributes to the same node/link service accounting, and
control loss contributes to the packet-drop KPI.

## Topologies and supernode discovery

`peer-mesh` builds bounded cohort rings, same-interest shortcuts, cross-cohort
links, and attacker exposure. `hybrid-supernodes` builds a connected honest
supernode mesh and lets normal peers select honest and false candidates using:

- `bootstrap`: configured honest bootstrap candidates;
- `interest-affinity`: candidates ranked by matching self-claimed subscription
  cohort;
- `exploration`: seeded selection across all candidates;
- `mixed`: one honest bootstrap link plus exploration and claimed-cohort
  affinity.

Hybrid scenarios also give one persistent attacker and one fresh-Sybil control
a bounded ingress link to an honest supernode. Those two workload-pressure
links traverse the normal subscription and inv/want paths but are not counted
as normal-peer supernode discovery selections.

Interest-affinity discovery is **not** verified
`nostr-social-graph` evidence. Cohort IDs are self-claimed, attackers may claim
ordinary cohorts, and the simulator reports honest/false selections so this
strategy is not mistaken for a trust oracle. Verified social-graph processing
is used separately by event and mesh admission. Discovery precision, honest
coverage, and false-only selection describe initial topology construction only;
the simulation does not perform runtime rediscovery. Link recovery restores the
same selected neighbors.

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
  admit/remove/readmit completion, removal latency, complete and disjoint
  human/machine trust-edge counts, signed graph updates, and per-transition
  human lifecycle checks;
- scheduled subscription messages and bytes, reliable-control resend and
  recovery counts (`subscription_retries` and
  `subscription_retry_recoveries`), rejection, bounded eviction, and
  close/reopen behavior;
- initial-topology discovery precision and honest coverage, false-only
  selections, supernode maximum/mean load, and load Gini concentration.

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
complete interested deliveries rather than merely absorb control or spam.

The kind-wide kind 37196 paid-offer subscription is intentionally broad, so
filter matching alone cannot distinguish an honest offer from a Sybil-signed
offer. Its per-class result represents an economic-admission gap, not a solved
incentive: Cashu or other economic proof, Cashu/Lightning settlement, multihop
payment routing, prices, utilities, and strategic equilibrium are outside this
simulation. The link/role ledgers and delivery credits preserve the underlying
service facts so those strategies can be compared later without replacing the
network model.

## Running 1,000 nodes

The default is 1,000 nodes, including 200 attackers, and runs both topologies
under all three peer-selection modes:

```sh
cargo run --release -p nostr-pubsub-sim
```

The ignored release gate runs a deterministic 18-case matrix: three seeds,
both topologies, and all three peer-selection modes, with 1,000 nodes per case.
It requires at least 95% aggregate and 90% worst-cohort legitimate delivery,
at least 60% peer-mesh and 50% mixed-hybrid signed-spam outcome suppression,
at least 50% suppression in the persistent machine-admitted identity lane,
zero honest-observer machine-removal false positives, exact accounting
conservation, and no shared-policy delivery regression against the neutral and
local controls. Fresh machine-admitted Sybils are compared with the lossy local
control rather than treated as learned identities.

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

To compare supernode discovery assumptions, repeat a fixed seed and all other
arguments with `--topology supernodes` and each of `--discovery bootstrap`,
`interest-affinity`, `exploration`, and `mixed`. The legacy `social-graph`
spelling remains accepted as a CLI alias.
