# Ad hoc mesh follow-up — 2026-09-10

This continues the [native mesh audit](mesh-audit-2026-09-09.md), focusing on
configurable trust, idle traffic, and repeatable release resource checks.
Implementation and release evidence are recorded separately below.

## Configurable trust and peer selection

One general trust prior means an initial preference for peers, adjusted by
local observations. The existing `PeerReputationConfig::trusted_raters` is an
optional set of public keys in npub or hex form; it defaults to empty. No
personal identity is selected automatically. These entrypoints supply signed
machine ratings. They do not automatically import ordinary personal follow
lists or map social profiles to transport identities, and confer no access to
private application data.

Positive service ratings previously allowed their subjects' third-party
ratings to affect the projection. Two causal regressions reproduced that
implicit delegation. The adapter now limits rating authority to the local root
and configured raters, preserving root revocation and replay behavior. This
uses the existing configuration, with no new score or delegation protocol.
Generic `nostr-social-graph` behavior is unchanged. The adapter passed 21 unit
tests and 14 integration tests after the correction.

The high-level `FipsPubsubClient` previously ignored the peer-quality policy
available to its lower-level mesh. A three-endpoint regression reproduced
selection by identity instead of the supplied quality score. The client now
accepts optional peer and event policies through `start_with_policies`, and
`FipsPubsubPolicy::client_policies()` exposes its shared projection. The existing
client options and event-only API remain compatible.

Connection selection and inventory fanout reuse the same bounded selector as
the lower-level mesh, retaining configurable unknown-peer capacity. Explicit
routed destinations keep priority when admitted. A live policy change can
replace a revoked peer and deliver through the existing subscription without
disconnecting either application-owned FIPS link. Policy output cannot replace
an authenticated peer identity. These focused cases and the library release
gates passed; downstream adoption remains in progress.

The managed `start_with_reputation` constructor owns one bounded rating
subscription and a paced maintenance task. Local ratings apply even after a
publication failure, and a failed send cannot starve later observations in the
same bounded batch. Causal tests reproduced both failures. All 43 FIPS adapter
library tests pass, including real multihop and WebSocket restart recovery.
The same 43 tests also pass against released FIPS core 0.4.79 and TCP endpoint
0.2.15. Strict workspace Clippy, formatting, and documentation compilation pass.
The final transport dependency update changes only those two lock entries.
The unchanged TypeScript package also passes its checks, 91 tests, build and
package inspection; this follow-up does not change its version or source.

## Idle session reports

Production-path tests reproduced session measurement reports triggering further
reports after application traffic settled. The candidate FIPS change suppresses
that report-only feedback while continuing to count every frame. Fresh traffic
resumes reporting. Link reports, path-MTU confirmation, and session idle
deadlines remain intact.

The causal test failed on the released implementation and passed after the
change. Eleven owner-path cases and 126 existing measurement, route, retry,
restart, rekey, idle-purge and path-MTU checks passed. A controlled native
baseline/candidate/baseline/candidate comparison also passed all four runs.
It reused the exact same Chat, Drive and Hashtree source pins and locked
dependency graphs; only three FIPS core production files changed. These were
debug builds, not optimized binaries or mobile battery measurements.

| Combined transit traffic | Released baseline | Candidate |
| --- | ---: | ---: |
| Before interruption | 2615–2776 B/s | 1794–1800 B/s |
| After recovery | 2841–2926 B/s | 2161–2243 B/s |

Mean traffic fell 33.33% before interruption and 23.65% afterward, or 28.33%
across the measured idle windows. CPU ranges overlapped, so these results do
not establish a general CPU improvement. Every run delivered four signed
events and two verified blobs through the transit peer with zero public relays.
Recovery took 13.37–13.56 seconds for the baseline and 13.41–13.70 seconds for
the candidate. All processes were reaped after the bounded comparison.

FIPS core and endpoint 0.4.79 include this change. All 2,408 local functional
tests passed, with four existing skips; all 43 hosted CI jobs and four platform
package workflows passed. Registry archives match the verified packages.
Anonymous canonical source verification retained all 71 advertised refs,
matched all 20,455 reachable Git objects, and passed strict integrity checks.

## Adversarial simulation

All three optimized release gates passed: an 18-case matrix with 1,000 nodes
and 200 attackers, four discovery comparisons, and a tenfold-load retained-state
check. The shared-reputation runs use the same explicit rater indices in every
mode, topology, and seed; three configured raters are deliberately adversarial.
They are not selected using hidden knowledge of honest or malicious roles.

| Shared reputation topology | Aggregate delivery | Spam suppression |
| --- | ---: | ---: |
| Peer mesh | 99.90–100% | 54.33–58.41% |
| Hybrid | 99.94–100% | 53.84–53.87% |

Ordinary honest ingress drops and ordinary honest-observer false removals were
zero. Explicit lifecycle-control probes retain separate accounting: a regression
sends both a lifecycle probe and ordinary traffic from the same rejected source
and verifies that the ordinary drop cannot be hidden. Aggregate admission and
byte counters are unchanged.

Service-only endorsement grants no authority and causes zero poisoning removals.
The separate compromised-authority probe still causes 794–798 removals when an
observer explicitly trusts the attacker. Configuration limits who can influence
trust; it does not make a chosen rater truthful. These assumptions differ from
the earlier audit, so its results are not a like-for-like improvement baseline.

Increasing spam events from 64 to 640 and fake inventories from 888 to 8,448
left maximum retained state at 129 entries and protocol-content p95 at 9,842
bytes. Existing delivery, cache, subscription, rediscovery, signature-work and
quiescent-queue limits remain enforced. These deterministic resource bounds are
complemented by real-process CPU and wire measurements; they are not hardware
battery measurements.

## Resource checks in releases

Iris Stack source `c6035a6343c569d480f407d7f47fc755cb825b64` is published and
verified through an anonymous cold clone. Its reusable workflow checks out the
exact lab source, including when another repository calls it. Release gates
can select an exact public Hashtree candidate revision, as well as exact Chat
and Drive revisions, before candidate artifacts are published.

The Linux gate requires all CPU measurements; missing data cannot silently
pass. The 5% per-process CPU and 4 KiB/s combined transit limits are unchanged.
A successful run records the lab revision, product source pins, executed
binary hashes, and measured results. Failed invocations remove prior success
receipts. Chat and Hashtree release workflows now require this gate; their
local publication paths, and Drive's local publisher, require a matching
receipt. The local receipt guards validate provenance consistency, not a
cryptographic signature over a user-supplied receipt.

The hosted gate passed against the previously released product pins. Required
Linux CPU samples ranged from 0.333% to 0.400% of one core per process; combined
transit traffic was 2601 and 2883 B/s in the two idle windows. All four signed
events and two verified blobs passed with zero public relays, including route
recovery in 13.18 seconds. This validates the new release gate and its baseline
inputs; pending application changes still require their own exact-source runs.

## Release state

FIPS 0.4.79 is released; the pubsub and application follow-ups remain in progress.
App Store submission is
authorized alongside TestFlight for affected iOS products. Apple readback on
2026-09-10 reports the prior Chat build 2026.9.900 as ready for distribution.
Drive 0.1.36 build 1037 is internal-only; its next Store submission needs an
App Store-eligible upload. Nostr VPN's existing release task retains ownership
of its frozen candidate and product release.
