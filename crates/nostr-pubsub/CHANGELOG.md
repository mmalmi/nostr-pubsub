# Changelog

## 0.1.20 - 2026-10-30

- Add a bounded, per-source history cursor with inclusive time bounds, dense-timestamp expansion, explicit partial completion, and cancellation tokens. Existing event buses and one-shot queries are unchanged.

## 0.1.18 - 2026-10-20

- Encode and verify standard Nostr OK receipts for exact-event remote provider admission. Keep local cache publication reports distinct from remote receipts.

## 0.1.17 - 2026-10-14

- Add a bounded signature verification cache for repeated events. Cache hits still verify the complete event body and require the exact previously verified signature.

## 0.1.16 - 2026-10-04

- Expose optional live subscription delivery status through providers and the
  shared router, preserving compatibility with providers that do not report it.
- Report bounded delivery gaps explicitly so consumers can reconcile against
  their authoritative event history before resuming live subscriptions.

## 0.1.15 - 2026-09-11

- Reduce peer-selection allocations when all candidates fit and when reserving
  capacity for unknown peers. Preserve deterministic ranking, duplicate-score
  handling, exclusions, and the existing unknown-peer reserve.

## 0.1.13 - 2026-07-18

- Add one transport-neutral router model for Hashtree/local indexes, FIPS
  peers, generic peers, and traditional Nostr relays.
- Query additive datasets concurrently, fail over ordered replicas, isolate
  source failures, and merge verified event IDs with complete provenance and
  one deterministic newest-first limit.
- Route noisy live subscriptions through the same source policy, globally
  deduplicate event IDs, and fan one close across every selected source.
- Add an owned `NostrPubsubRouter` implementing query, publish, live subscribe,
  and `PubsubProvider`, so daemons can store an explicit modular router without
  a consumer-local wrapper.

## 0.1.12 - 2026-07-18

- Add subscription-scoped `INV` and `WANT` extensions to the existing Nostr
  `REQ`/`EVENT`/`CLOSE` FIPS wire codec.
- Group all matching subscription IDs for one peer into one `INV`; keep full
  event transfer as an ordinary subscription `EVENT`, allowing live
  mesh receivers to select one provider without changing subscription IDs,
  filters, or close semantics.
- Keep each `WANT` scoped to exactly one event ID; the provider selects a
  still-open matching subscription for the ordinary `EVENT` response.
- Expose exact peer-subscription lookup for validating requested live events.

## 0.1.11 - 2026-07-16

- Expose bounded mesh snapshots and verified snapshot seeding so reliable
  carriers can reconnect and replay without duplicating pubsub state.
- Include the configured Inv/WANT envelope in the maximum wire-size bound.
