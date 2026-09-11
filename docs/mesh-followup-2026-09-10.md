# Ad hoc mesh follow-up — 2026-09-10

This follows the [native mesh audit](mesh-audit-2026-09-09.md), updated September 11.
Configurable trust, shared shutdown, bounded service retries, lower idle traffic
and Android gateway fixes are released in the libraries. Chat, Nostr VPN and Drive
have shipped their updates. Drive's final desktop correction keeps direct device
approval responsive during slow relay publication. It passes 1,052 host tests,
packaged resource checks, Mac–Android linking and the full native workload's five CPU
checks. A cleanup-guard failure and successful independent restoration remain
recorded separately. Drive's eligible iOS build is available internally in TestFlight
and submitted for external beta and App Store review.

## Current releases

| Component | Published version |
| --- | --- |
| FIPS core / endpoint | [0.4.81](https://crates.io/crates/nvpn-fips-core/0.4.81) / [0.4.81](https://crates.io/crates/nvpn-fips-endpoint/0.4.81) |
| FIPS TCP endpoint | [0.2.16](https://crates.io/crates/nvpn-fips-tcp-endpoint/0.2.16) |
| Core pubsub | [0.1.15](https://crates.io/crates/nostr-pubsub/0.1.15) |
| Pubsub social-graph adapter | [0.2.3](https://crates.io/crates/nostr-pubsub-social-graph/0.2.3) |
| Pubsub FIPS adapter | [0.5.4](https://github.com/mmalmi/nostr-pubsub/releases/tag/nostr-pubsub-fips-v0.5.4) |
| Hashtree FIPS transport | [0.4.19](https://crates.io/crates/hashtree-fips-transport/0.4.19) |
| Hashtree resolver | [0.2.85](https://crates.io/crates/hashtree-resolver/0.2.85) |
| Hashtree CLI / embedded | [0.2.150](https://github.com/mmalmi/hashtree/releases/tag/v0.2.150) / [0.2.92](https://crates.io/crates/hashtree-embedded/0.2.92) |
| Iris Chat | [v2026.9.10.1](https://github.com/irislib/iris-chat-rs/releases/tag/v2026.9.10.1) |
| Nostr VPN | [4.1.10](https://github.com/mmalmi/nostr-vpn/releases/tag/v4.1.10) |
| Iris Drive | [0.1.37](https://cdn.iris.to/npub1xdhnr9mrv47kkrn95k6cwecearydeh8e895990n3acntwvmgk2dsdeeycm/releases%2Firis-drive/v0.1.37/release.json) |

Registry downloads match the verified library archives and published source.
Hashtree's latest release passes all nine source checks, five
platform builds, packaged Windows startup, payment/resource gates and public
macOS/Linux/Windows installation checks. Its canonical downloads, immutable GitHub
assets and Homebrew installation are verified. Chat's 14 assets and VPN's 15 assets
also have verified supported distribution, including canonical downloads and Zapstore.

Fresh Apple reads at September 11, 17:14 UTC verify the exact eligible builds and
matching Store attachments below. Automatic release after approval remains set for
all three. Store state does not establish availability in every storefront.

| Product | iOS version / build | App Store | Internal / external TestFlight |
| --- | --- | --- | --- |
| Chat | 2026.9.1001 / 2026091001 | Ready for distribution; review complete | Active / active |
| Nostr VPN | 4.1.10 / 4001016 | Ready for distribution; review complete | Active / active |
| Drive | 0.1.37 / 1038 | Waiting for review | Active / waiting for beta review |

## One trust prior, explicit rating authority

A general trust prior provides an initial peer preference, adjusted by local
observations. `PeerReputationConfig::trusted_raters` accepts optional npub or hex
public keys and defaults to empty. No personal identity is selected automatically.
Applications expose the same bounded key validation:

| Product | Optional setting |
| --- | --- |
| Hashtree | `nostr.fips_trusted_raters` |
| Chat | `IRIS_CHAT_FIPS_TRUSTED_RATERS` |
| Drive | `IRIS_DRIVE_FIPS_TRUSTED_RATERS` |

Configured entrypoints supply signed machine ratings. They do not import personal
follow lists, map social profiles to transport identities or grant access to private
application data. Generic `nostr-social-graph` behavior is unchanged.

Causal regressions found that a positive service rating could also give its subject
authority to rate third parties. Rating authority is now limited to the local root
and explicitly configured raters. This keeps one reputation model while separating
an endorsement from delegation; it adds no extra score or delegation protocol.
Revocation and replay checks remain intact. Adapter checks pass, including the
actual three-node signed-event flow and serialization/startup validation.

The high-level `FipsPubsubClient` now applies the lower-level mesh quality policy.
`start_with_policies` accepts peer/event policies, while
`FipsPubsubPolicy::client_policies()` exposes their shared projection. Connection
selection and inventory fanout use the same bounded selector, with unknown-peer
reserve capacity and priority for admitted explicit routed destinations. Live
revocation can replace a selected peer and deliver through an existing subscription;
policy output cannot replace authenticated identity or close application-owned links.

`start_with_reputation` owns one bounded rating subscription and paced maintenance
task. Tests verify that local observations apply after publication failure and
one failed send cannot starve the remaining batch. The unchanged TypeScript package
passes checks, 91 tests, build and package inspection.

## Transport, lifecycle and resource fixes

**Shared shutdown.** A real embedded Hashtree server and Chat's no-relay host-BLE
runtime reproduced three subscriptions surviving shutdown when another reference
retained the client. `shutdown_shared(&self)` now closes admission, aborts and joins
owned tasks, coordinates concurrent callers and preserves unfinished joins when
a waiter is cancelled. Consuming `shutdown(self)` remains available. Already
accepted events can drain before EOF. Chat awaits this cleanup before stopping its
endpoint; Drive removes a redundant ownership-unwrapping branch. Tests cover retained
controllers, live WebSockets, Nearby/device sync and encrypted multihop delivery.

**Service retry pacing.** Full-client tests reproduced seven reset-rejected attempts
in 1.3 seconds and 27 accepted-then-closed streams when subscription activity also
sent requests. Repeated attempts now share a monotonic three-second deadline;
first connections remain immediate. Send activity and short-lived link refreshes
cannot bypass it, and deselection removes the retained retry state. Persistent
subscriptions recover when the service returns. A default 500 ms one-shot query can
expire during recovery and may need retry; Hashtree normally allows 5.5 seconds.
Identity decoding is deferred until admission, and redundant helpers are removed.

**Less allocation.** Core pubsub reuses its sorted, deduplicated candidate list and
removes an unnecessary selected-peer set. A host two-peer probe falls from
13 allocations/1,136 requested bytes to 6/720; a capped seven-peer probe falls from
30/1,394 to 24/1,014. Exact order, exclusions, unknown reserve and revocation remain
tested. FIPS singleton crypto batches reserve only admitted work: item storage falls
from 101,376 to 792 bytes in the arm64 host test. Full batches and partial-batch
ordering remain intact. These are host allocation measurements, not Android CPU savings.

**Report feedback and rekey recovery.** FIPS measurement reports no longer trigger
further reports indefinitely after payload traffic settles. Every frame remains
counted, new traffic resumes reporting, and link reports, path-MTU confirmation and
idle deadlines remain intact. A separate causal test found that Tree-mode rekey
could lose route coordinates after a tree change even while cached TUN data flowed.
Three production lines reuse bounded route lookup; ten due checks share one request,
retain the old epoch, recover verified coordinates and then cut over both endpoints.
FIPS 0.4.81 passes 2,412 local tests with four skips, 43 hosted jobs and four platform
workflows. Hosted staggered rekey records 9,313/9,313 replies, no misses or duplicates
and two rotations per session. That fixture suppresses unrelated periodic parent
reevaluation; broader routing scenarios remain unchanged. The Android CPU comparisons
predate this rekey correction and cannot establish its resource impact.

## Hashtree and application corrections

An unavailable optional same-user blob route could prevent all Drive FIPS startup.
The corrected boundary omits only that route while preserving its error and the
healthy Drive store. The same causal fixture then moves signed roots and verified
file bytes both ways without relays, Blossom or seeds. The full daemon matrix passes
25 cases with five existing ignores; the optional route fix does not diagnose an
earlier ambient database error.

Hashtree's mutable `Get` lookup now uses configured relays and observes the newest
usable signed root for ten seconds, retaining its subscription through empty EOSE
and closing its owned connection afterward. Immutable resolution remains immediate.
The same compiled fixture fails on the released behavior and passes the correction;
all 18 resolver tests and strict resolver checks pass. Existing CLI lint failures
remain documented: matched baseline/candidate checks add or remove no diagnostics.

Android's pinned Rust toolchain returned `Unsupported` for the profile transaction
lock before the embedded gateway could open. Hashtree uses Android-only `flock`
through its existing dependency, retaining real shared/exclusive exclusion,
nonblocking behavior, explicit unlock, errors and deadlines. Three actual-Android
lock scenarios fail on the old module and pass on the correction; host lock tests
also pass. A production NativeCore regression separately proves authorized-profile
gateway startup and the expected HTTP 307 redirect. This was a platform file-locking
failure, not a generic social-graph policy failure.

Drive also adds the exact `127.0.0.1` cleartext exception needed by its local gateway;
global network policy stays unchanged. The effective policy is checked on the tested
Android device, without claiming an API 28–36 red/green matrix. Two causal Android
service tests verify that the data-sync timeout callback stops the service even
when a newer start request exists. They exercise actual OS destruction after the
callback, without waiting for the six-hour allowance.

Windows visual inspection found a blank WPF window despite discoverable controls.
Matched runs on identical app bytes isolated the rendering path in the tested VM.
The optional `IRIS_DRIVE_WINDOWS_SOFTWARE_RENDERING=1` setting renders the real
interface through product code; the default stays unchanged. The exact driver cause
and a source regression are not established. Final packaged checks appear below.

The release exercise also fixed Homebrew publisher history replacement. The publisher
now adds a descendant to the existing complete snapshot, preserves other refs/files
and refuses failed history reads or changed refs. Real-Git regressions and actual
public installation checks pass. This release-tool correction adds no Homebrew
channel for Drive.

## Controlled traffic and adversarial simulations

The controlled baseline/candidate/baseline/candidate comparison holds Chat, Drive,
Hashtree and their locks constant, changing only three FIPS production files. All
four debug-backend runs deliver four signed events and two verified blobs through
an uninterested Hashtree transit peer with zero public relays. Each uses 15-second
idle windows:

| Transit traffic | Released baseline | Candidate |
| --- | ---: | ---: |
| Before interruption | 2,615–2,776 B/s | 1,794–1,800 B/s |
| After recovery | 2,841–2,926 B/s | 2,161–2,243 B/s |

Mean traffic falls 33.33% before interruption and 23.65% afterward, or 28.33%
across these windows. Recovery remains 13.37–13.56 seconds for baseline and
13.41–13.70 seconds for candidate. CPU ranges overlap. Traffic sums FIPS peer
sent/received counters; it excludes TCP/IP headers, lower-layer retransmissions
and unrelated interface traffic. No general CPU or battery improvement is proved.

All three optimized simulation gates pass: an 18-case matrix of 1,000 nodes with
200 attackers, four discovery comparisons and a tenfold-load retained-state check.
Explicit rater indices are identical across modes, topologies and seeds, including
three deliberately compromised configured raters. Selection has no hidden knowledge
of honest/attacker roles.

| Shared-reputation topology | Delivery | Spam suppression |
| --- | ---: | ---: |
| Peer mesh | 99.90–100% | 54.33–58.41% |
| Hybrid | 99.94–100% | 53.84–53.87% |

Ordinary honest ingress drops and honest-observer false removals are zero. Lifecycle
probes have separate accounting; a paired test prevents rejected ordinary traffic
being hidden as probe traffic. Service-only endorsement causes zero poisoning
removals, but explicitly trusting compromised raters still causes 794–798 removals.
Configuration limits authority, not dishonesty. Changed anchor assumptions prevent
a like-for-like comparison with the earlier audit. Raising spam events 64→640 and
fake inventories 888→8,448 keeps retained state at 129 entries maximum; protocol
content p95 is 9,842 bytes and maximum 10,874 bytes. Delivery, cache, subscription,
rediscovery, signature-work and quiescent-queue limits remain enforced.

## Drive 0.1.37 source and coverage

The release source is `5f8c147ebd1b354c61a1be25f3481bdccb2989e1`. Its full host gate
passes 1,052 Rust tests with five intentional daemon ignores, strict lint and release
workflow checks. Its annotated release tag passes fresh anonymous verification of all
18,930 reachable objects and strict integrity checks, preserving all 13 preceding
refs and adding the release tag.
New desktop artifacts pass packaging, payload, functionality and resource checks.
The complete five-instance native workload and CPU assertions also pass; its outer
cleanup failure and independent recovery are recorded below.

All nine dependency tuples are verified through ordinary locked builds. Both jobs
in the [exact-source cohort](https://github.com/irislib/iris-stack/actions/runs/34615975935)
pass on the first attempt with Chat `dc524968bab6b93d770cc1d7e8e81414293c37ad`,
Hashtree CLI 0.2.150 and lab `cec9501659b9cf08f9600e3f14987cad6ca1e7d3`.
Four signed events and two verified blobs recover through the three-process topology
with zero public relays. Two 65-second windows cover the one-minute reputation
maintenance: per-process CPU is 0.17–0.33% against 5%, and combined transit FIPS
counters are 2,185 B/s before partition and 721 B/s after recovery against 4,096 B/s.
Event recovery takes 13.269 seconds; the rejoined blob takes 46 ms. The downloaded
receipt matches the hosted artifact digest and passes the production source/limit
validator. These two phases are not a causal A/B comparison or a cryptographically
signed local receipt. Managed pubsub has separate functional/shutdown tests; the
transit peer's Nostr service is disabled, so this is not blanket service coverage.
Traffic is measured from FIPS counters, excluding TCP/IP and other interface overhead.

The following table records the refreshed desktop checks. Mobile artifacts retain
their original `cd5c9bc` source: 597 tracked mobile production and shipping inputs,
including all manifests and locks, are unchanged in `5f8c147`. Android's broader
incremental-build watch pattern does include the changed desktop CLI files; this
reuse proof concerns the actual mobile dependency closure, not every watched input.

| Artifact check | Recorded result | Coverage boundary |
| --- | --- | --- |
| Signed Mac artifacts | Build, signing, notarization and independent payload checks pass | Packaged launch fallback observes process liveness |
| Signed Mac app + daemon idle | 0.46% / 1.35% means; limits 5% / 10%; 12 samples each | Active FIPS observed; provider CPU not sampled |
| Earlier Mac GUI functionality | Linking, import and backup checks pass | Separate preceding Debug app; fresh signed linking is recorded below |
| Packaged Windows GUI + daemon | Functional/visual checks pass; idle means 0.92% / 0.50%, limits 5% / 10%; 12 samples each | Configured startup fixture, resolver/discovery disabled; two direct peers; both peaks 6% |
| Packaged Linux GTK + daemon | Functional/visual checks pass; idle means 0.10% / 0.22%, limits 5% / 10%; 12 samples each | Configured startup fixture, resolver/discovery disabled; zero peers |
| Android final native library | Ten functional tests, gateway, approval/link/sync and provider checks pass | UI-test shell with optimized Rust; separate from shipping APK |
| Android signed shipping APK | Normal UI profile reaches Ready and 1/1 online; mean 3.67%, peak 4.54%, limit 5% over 60.46 seconds after 90-second warmup | 11 intervals, same process/device identity; single-device startup idle, private FIPS heartbeat unavailable |
| Release iOS simulator | App mean 0.82%, limit 10%; 12 samples with authorized native state, gateway and FIPS checks | Production host-process method; no physical-device or provider CPU acceptance |
| Debug iOS simulator | GUI linking and local Blossom handoff pass; six events, three verified blocks | Separate functional evidence |
| Physical iPhone | Iris Apps install/launch and fresh WebView probe pass; opens in 16.901 seconds under 40 seconds; gateway and HTTP checks pass | Development-signed app, Release Swift with probe hooks and Debug Rust; separate from the Store IPA and physical CPU coverage |
| Mac–Android manual linking | Both directions pass at 4.042 / 6.291 seconds, under 15 seconds; authenticated FIPS, durable acknowledgement and provider exchanges | Signed `5f8c147` Mac app and frozen `cd5c9bc` Android UI-test app with normal UI entry; independent restoration checks pass |
| Five-instance daemon composition | GUI approvals, full rosters, file operations, concurrent edits, restarts, 32-file batch and 256 KiB file pass; all five CPU checks pass | Ubuntu/macOS/two local-role means 0.91%/4.69%/1.91%/1.47%; Windows formatted counter reports 0%; 12 samples each, limit 10%. Mobile labels are host daemons. Outer cleanup guard failed, with independent restoration verified |

The Windows build and packaging command completed successfully, but its strict
guard failed on a lingering compiler helper, which was reaped. Separate verification
then accepted all 468 packaged files and the actual installer payload before the
GUI and resource checks; the original guard failure remains recorded. Native cleanup and prior failed
attempts have separate evidence rather than being relabelled. The Android shipping
result independently passes
signature, version 0.1.37/build 1038, full archive manifests, license and exact JNI
checks. Its temporary production install/profile is removed afterward, restoring
original absence and preserving the existing debug/test apps. The native library
is packaged unstripped, increasing its size. Earlier failing or near-limit Android
measurements used different candidates or fixtures; the final result is not a
causal CPU-improvement comparison.

The full native workload uses a three-minute warmup and one-minute CPU sample for
each role. Windows requires the same nonempty daemon PID set and a valid formatted
counter at every query; missing data cannot produce a passing zero. Its reported
zero does not prove zero work, and this sampler does not retain raw CPU-time deltas
or verify counter timestamps. The native workload proves restart recovery; the
explicit network partition and rejoin test is the separate hosted cohort above.

After the native workload exited successfully, the outer guard detected a remaining
process group and failed. It terminated and reaped that group; independent checks
then confirmed task profiles, processes, mounts and registrations were absent and
existing user state was preserved. The residual process was not identified, so its
cause remains unknown. Release acceptance uses the passing workload/resource results
and independent cleanup proof while retaining the failed outer result. Capturing
owned group identities and checking every cleanup outcome would improve diagnostics.

All nine final Mac/Linux/Windows/Android release assets are published through the
existing channels. Independent checks verify all 18 versioned/latest download bodies,
matching manifests, the Zapstore APK body and three signed metadata events. The
release tag and preceding source refs are preserved. No new GitHub Release or
Homebrew channel is introduced for Drive. The eligible iOS 0.1.37/build 1038 is
available internally in TestFlight and submitted for Store and external beta review;
submission does not establish public availability.

Actual Store submission exposed a release-helper defect: review-item reads omitted
the version relationship, so the exact-version check stopped before submission.
Both reads now request that relationship explicitly. A matching API fixture makes
eight existing tests fail on the old helper and all 23 pass with the correction.
The corrected helper resumes the same review without another version or upload,
preserving the build, item and review-state checks.

The Mac release smoke helper also lost its mounted-image state in a shell
subprocess, preventing its exit handler from detaching the image. It now retains
that state in the parent shell. Regressions cover successful copying and copy
failure, and the fast verification gate passes.
These two release-helper corrections are separate from the shipped product source,
at `52db1fd29d640a8eda2d1faa208ce90327b25801`. The `v0.1.37` tag and its
verified binaries retain the frozen source described above.

Native linking also exposed a desktop daemon responsiveness defect. A roster-send
branch awaited FIPS delivery and then optional relay publication inside the main
event loop, for up to two plus five seconds. In a failing run, the authenticated
direct connection was already established, but repeated relay timeouts delayed the
reported FIPS state. This native fixture uses the normal public relay defaults,
unlike the isolated no-relay cohort above. One owned background sender now retains the existing delivery cache, retries and
timeouts without holding up the main loop. A two-daemon regression fails on the
unchanged behavior and passes with the correction: a direct ACK and fresh FIPS
status progress in 2.351 seconds while the relay withholds its ACK. Relay recovery
then succeeds without a redundant direct resend. Fifteen focused roster unit tests
and strict lint also pass. Seven desktop artifacts and the exact-source cohort were
then rebuilt and verified. These results do not establish a causal CPU or bandwidth
improvement. Unchanged mobile artifacts retain their original provenance.

## Remaining limits

Native tests prove multihop delivery and recovery over tested links, not browser
cold start without bootstrap or success through every firewall/radio topology.
The full native composition has the qualified acceptance described above.
Standalone iPhone smoke and optional share-extension XCTest
were not run; the required physical Iris Apps probe includes installation and launch. CPU limits are regression ceilings on
tested hardware, not idle targets, continuous connectivity traces or battery claims.
Missing/short/non-finite samples cannot silently pass.

Five daemon-matrix cases remain ignored: provider mutation replay while stopped,
live concurrent-edit partitions and three transfer benchmarks. The Mac idle sample
does not prove Finder operations; shipping Android's release sandbox limits native
transport observability. Bandwidth claims remain confined to the controlled FIPS
counter measurements above.
