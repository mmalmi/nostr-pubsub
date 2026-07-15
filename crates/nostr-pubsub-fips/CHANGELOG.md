# Changelog

## Unreleased

- Add a bounded, sans-I/O Inv/WANT record layer for reliable `fips-tcp`
  carriers, with split/coalesced large records and bounded retained input.
- Apply event admission before cache, delivery, or forwarding and peer policy
  before queueing traffic.
- Restore verified durable snapshots into the existing bounded mesh cache and
  replay inventories on every peer connection or reconnection.

## 0.3.0 - 2026-07-15

- Move the adapter to `fips-core 0.4.0` without changing its bounded FSP
  datagram protocol.
- Keep external peerfinding on the application-provided `EventBus`; the
  adapter neither opens relay sockets nor adds an adapter-local workaround.
- Advertise `nostr.pubsub/1` only for the lifetime of the registered FSP
  service.
