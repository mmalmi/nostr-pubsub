# Changelog

## 0.4.4 - 2026-07-20

- Subscribe every high-level FIPS pubsub client to default `fips-overlay-v1`
  kind `37195` endpoint adverts, publish the local signed advert into bounded
  replay, refresh it at half its signed TTL (at most every 30 minutes), and
  ingest received adverts through the transport-neutral FIPS validator without
  opening Nostr relay sockets. Reserve this internal subscription in addition
  to the configured application subscription limit.
- Add optional shared social-graph event admission at the FIPS pubsub boundary,
  before a received event enters local delivery, replay, or multi-hop gossip.

## 0.4.3 - 2026-07-19

- Keep separate bounded accepted-event and observed-ID caches. Structurally
  valid full events and inventory claims are observed per authenticated peer
  and subscription epoch before policy acceptance, with 1,024 IDs per scope
  and a 16,384-ID aggregate ceiling.
- Suppress repeated out-of-filter delivery, score objective provider
  misbehavior with decay, and disconnect providers behind a bounded reconnect
  cooldown after sustained abuse. Malformed records count more strongly;
  isolated filter races do not affect event authors or social reputation.
- Quiesce idle FIPS pubsub transport work, bound provider retries, and expose
  wire/TCP/cooldown counters without changing the reliable INV/WANT/EVENT
  protocol.
- Expose policy checks for already verified Nostr events so callers retain the
  verified object across admission and avoid duplicate signature validation.

## 0.4.2 - 2026-07-18

- Retry `WANT` against its sole advertised provider until the ordinary
  addressed `EVENT` arrives; alternate providers still rotate first. This
  closes live mesh delivery gaps when a queued request coincides with a
  FIPS-TCP connection transition.

## 0.4.1 - 2026-07-18

- Use grouped `INV`, one-event `WANT`, and ordinary addressed `EVENT` for both
  bounded historical replay and new live events over reliable FIPS-TCP.
- Depend on `nostr-pubsub` 0.1.13 so FIPS sources compose with the shared
  historical/live router used by Hashtree indexes and traditional relays.

## 0.4.0 - 2026-07-18

- Carry ordinary Nostr `REQ`, `EVENT`, and `CLOSE` frames exclusively over
  reliable `fips-tcp` on FIPS service port 7368; remove the raw-FSP datagram
  carrier and compatibility fallback.
- Add grouped subscription-scoped `INV`/`WANT` live delivery: duplicate inventories
  from many peers and open subscriptions select one provider and fetch one
  ordinary `EVENT`, then fan it out to every matching local subscription.
- Bound pending inventory, alternate-provider retry, replay, peer, record, and
  byte state; expose delivery counters for deterministic mesh observability.
- Preserve the low-level generic Inv/WANT TCP driver for applications that do
  not use Nostr subscription semantics.

## 0.3.2 - 2026-07-18

- Add an explicit excluded-transport set for applications whose FIPS pubsub
  carrier must not recursively select the transport it is carrying.
- Keep every other authenticated connected peer eligible; default clients
  retain the existing all-transport behavior.

## 0.3.1 - 2026-07-16

- Add a bounded, sans-I/O Inv/WANT record layer for reliable `fips-tcp`
  carriers, with split/coalesced large records and bounded retained input.
- Apply event admission before cache, delivery, or forwarding and peer policy
  before queueing traffic.
- Restore verified durable snapshots into the existing bounded mesh cache and
  replay inventories on every peer connection or reconnection.
- Add the manually driven `fips-tcp-endpoint` production carrier with generic
  service namespace/version, bounded partial-write queues, deterministic
  duplicate-stream selection, and explicit reconnect ownership.
- Exercise two real FIPS endpoints through large split records, coalesced
  records, simultaneous and late connection, forced reconnect, replay, and
  queue pressure.

## 0.3.0 - 2026-07-15

- Move the adapter to `fips-core 0.4.0` without changing its bounded FSP
  datagram protocol.
- Keep external peerfinding on the application-provided `EventBus`; the
  adapter neither opens relay sockets nor adds an adapter-local workaround.
- Advertise `nostr.pubsub/1` only for the lifetime of the registered FSP
  service.
