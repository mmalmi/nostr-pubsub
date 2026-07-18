# Changelog

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
