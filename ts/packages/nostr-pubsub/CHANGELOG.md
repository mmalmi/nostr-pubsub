# Changelog

## 0.5.15 - 2026-10-08

- Update the shared TCP/FIPS transport to 0.2.3 so receiver buffer-space updates
  do not trigger unnecessary retransmission or preserve stale duplicate ACKs.

## 0.5.14 - 2026-10-02

- Add opt-in NIP-77 gap recovery alongside immediate cache reads and ordinary
  live relay requests, using the same socket and verified event admission.
- Bound capability lookup, inventory, missing IDs, frames, rounds, concurrency
  and deadlines; unsupported or failing relays retain ordinary subscriptions.
- Preserve requested historical windows and cancellation without claiming
  all-history sync or uploading local-only events.

## 0.5.13 - 2026-09-30

- Default relay requests to at most 20 exact OR filters so ordinary batches work
  with common relay limits. Larger batches remain an explicit application option.
- Cover rejected over-limit history and explicit larger limits with real socket
  tests; retain exact recipient matching and bounded subscription teardown.

## 0.5.12 - 2026-09-30

- Wait for event admissions queued during earlier cache writes before completing
  history. Queries retain all received events even when relay EOSE arrives while
  the persistent index is busy; cancellation and deadlines remain prompt.

## 0.5.11 - 2026-09-30

- Keep requested peer responses eligible during finite CPU/transport backlogs
  while trying alternate providers. The first valid requested response wins;
  unanswered work expires after ten seconds and remains capacity-bounded.

- Reject unadmitted peer datagrams before TCP allocates connection state, and
  treat a remote half-close released by a concurrent reset as already closed.

- Batch distinct FIPS interests as exact OR filters within the configured peer
  filter and frame bounds. Preserve independent local matching, recent history,
  and cancellation while sharing live/history interests on the same connection.
- Coalesce peer subscription changes across worker tasks and retire empty
  batches immediately. Async source subscription failures propagate to queries
  instead of leaking pending interests or claiming complete history.
- Exercise 512 peer interests and deliveries under a 128-subscription carrier
  cap, including recipient isolation, bounded teardown, frame limits, and aborts.

## 0.5.10 - 2026-09-30

- Enforce explicit relay/source delivery scopes and optional local-echo exclusion
  before event deduplication. Concurrent copies retain their separate source
  evidence while sharing one durable admission.

- Allow explicitly requested network history to return superseded replaceable
  versions without changing the latest-value cache or bypassing deletion and
  expiration checks.

- Report incomplete history when durable event admission fails, including every
  overlapping query sharing the failed write. Relay EOSE cannot hide an index error.

## 0.5.9 - 2026-09-30

- Coalesce relay subscription removals across worker tasks, retiring empty
  batches immediately so closing hundreds of interests does not send hundreds
  of replacement requests. Surviving interests retain exact local matching.

## 0.5.8 - 2026-09-30

- Bound FIPS historical queries by their configured observation window even
  when the caller allows a later overall deadline; empty peer history remains
  explicitly incomplete and does not delay relay-backed app queries.

## 0.5.7 - 2026-09-30

- Add a shared browser/worker Nostr runtime with exact-filter relay batching,
  explicit historical completion, overlapping reconnect recovery, authenticated
  relay support, scoped publication, and truthful remote acknowledgments.
- Add an application-owned persistent event store/outbox contract with
  replacement, expiration, and authorized deletion handling.
- Coalesce concurrent peer connection attempts and share matching live/history
  interests so application startup does not exhaust TCP or peer subscription bounds.
- Serve bounded retained local event history over the existing FIPS node,
  alongside other services such as Hashtree file sharing.
- Exercise 1,200 live interests over real relay sockets without widening
  recipient filters or duplicating matching deliveries.

## 0.5.6 - 2026-08-19

- Treat an authenticated TCP/FIPS stream that closes between a cleanup state
  check and its queued abort as already released, while preserving abort
  failures for connections that remain retained. This removes spurious
  `unknown connection` errors during simultaneous pubsub stream convergence.

## 0.5.5 - 2026-07-26

- Add an opaque exact-object verification boundary for shared
  `SimplePool` transports, allowing one injected synchronous verifier such as
  `nostr-wasm` to provide canonical admission without a duplicate Schnorr pass
  or trust in a spoofable public verification marker.

## 0.5.4 - 2026-07-26

- Reuse private canonical verification admission across adjacent protocol
  layers, avoiding duplicate Schnorr verification while rejecting spoofed
  public verification markers.

## 0.5.3 - 2026-07-26

- Add cancellable asynchronous batch verification admission for immutable
  events checked by an external or worker trust boundary, preserving the
  canonical verified-event fast path without repeating Schnorr verification.

## 0.5.2 - 2026-07-18

- Add the shared `SimplePoolNostrRelayTransport` browser relay carrier with
  boundary verification, bounded historical query completion, live
  subscriptions, and at-least-one-relay publication success.

## 0.5.1 - 2026-07-18

- Preserve a negative backend priority in the owned router when a relay is the
  only successful publish route instead of incorrectly flooring it at zero.

## 0.5.0 - 2026-07-18

- Replace the raw-datagram FIPS relay bridge with the reliable FIPS-TCP
  `nostr.pubsub/1` client on port 7368; `localPeerId` is now required for
  deterministic simultaneous-stream ownership.
- Use compatible grouped `INV`, one-event `WANT`, and ordinary addressed
  `EVENT` for both bounded historical replay and new live events, with global
  event-ID deduplication and alternate-provider retry.
- Add matching historical and live routers for Hashtree/local indexes, FIPS
  peers, and traditional Nostr relays; remove the legacy route `bus` alias and
  obsolete `FipsNostrRelayService` API.
- Add the owned `NostrPubsubRouter` for applications that need one reusable
  query/publish/live provider rather than one-shot route helper calls.
- Add shared Rust-process wire vectors and real FIPS-TCP coverage for all five
  `REQ`/`INV`/`WANT`/`EVENT`/`CLOSE` messages.

## 0.4.0 - 2026-07-18

- Split the read-only `NostrEventReader` and write-only `NostrEventPublisher`
  contracts while preserving the combined `EventBus` API and routed `bus` alias.
- Give source routes explicit dataset identities: replicas fail over within one
  dataset, while additive datasets are queried concurrently and merged.
- Deduplicate verified events with complete provenance, deterministic ordering,
  one global result limit, isolated source failures, and per-route/per-dataset
  outcome reporting.
- Propagate cancellation and one absolute deadline through routed and in-memory
  queries without activating any relay or changing publication policy.
- Honor NIP-01 OR semantics by applying each filter's limit independently and
  reserving `QueryOptions.limit` for the router-wide result cap.

## 0.3.1 - 2026-07-16

- Retry a local subscription's `REQ` after an initially unavailable FIPS route
  later reconnects, while keeping replay delivery valid during the pending send.
- Close a late successful `REQ` when its local subscription or peer admission was
  removed before the send completed.

## 0.3.0 - 2026-07-16

- Add `FipsNostrPubsubClient`, the shared browser `nostr.pubsub/1`
  `REQ`/`EVENT`/`CLOSE` carrier for signed Nostr events.
- Keep peer admission application-owned and explicit; connected peers are never
  inferred, and subscriptions refresh when admitted standalone links reconnect.
- Bound replay, subscriptions, filters, peers, frames, and pending work; failed
  publications remain retryable and invalid or non-admitted traffic is dropped.
- Match the native FSP datagram maximum exactly: accept 65,525 bytes and reject
  65,526, with a real Rust process roundtrip gate.
- Include TypeScript sources referenced by the published declaration and source
  maps so clean-installed artifacts are self-contained.

## 0.2.0 - 2026-07-16

- Add the bounded `FipsInvWantStream` and `FipsInvWantTcpDriver` over the
  shared TCP/FIPS v1 stack.
- Cover partial and coalesced records, queue and peer bounds, close/reset
  lifecycle, reconnects, and real Rust-to-TypeScript process interoperability.
- Match Rust simultaneous-connect ordering by normalizing compressed and
  x-only transport identities to their canonical npub ordering key.
- Keep authenticated capability discovery explicit; no fallback provider
  namespace or unauthenticated peer inference is introduced.

## 0.1.5 - 2026-07-13

- Add the authenticated FIPS Nostr relay adapter and shared Rust/TypeScript
  interoperability vectors.
