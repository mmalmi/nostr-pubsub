# nostr-pubsub-fips

`FipsEndpoint` provider for local-only Nostr pubsub. It uses the standard Nostr
`REQ`, `EVENT`, and `CLOSE` JSON messages on FIPS service port `7368`.
While that FSP service is registered it advertises the authenticated same-host
capability `nostr.pubsub/1`; dropping the client withdraws it.

The provider fans each subscription and publication to authenticated,
connected peers reported by FIPS, regardless of underlay transport.
Applications whose pubsub carrier wraps another FIPS transport can use
`start_excluding_peer_transports` to prevent recursive carrier selection while
leaving all other authenticated transports eligible.
Subscription replies are accepted only from the peers that received that
subscription. Each client also serves peer subscriptions and keeps a bounded
event cache, so late subscribers can receive announcements without a relay.
Its port-scoped receiver cannot consume another app service's datagrams on a
shared endpoint.
Frames, peer fanout, active subscriptions, replay deduplication, and delivery
queues are bounded; replay defaults to at most 8 events per filter.

## Reliable Inv/WANT byte streams

`FipsInvWantStream` is the sans-I/O foundation for applications that need
reliable, larger Nostr-event records over `fips-tcp`. It reuses the shared
bounded `InvWantMesh` instead of introducing an application-specific mesh:

- a four-byte length prefix frames records split or coalesced by the stream;
- record size, retained partial-input peers, receive work, mesh fanout, cache,
  pending routes, and replay are explicitly bounded;
- signed event admission runs before cache insertion, delivery, or forwarding;
- peer policy runs before records are queued or retained; and
- a durable application snapshot can seed the cache, whose inventories are
  replayed whenever a peer connects or reconnects.

The stream state machine owns no socket, task, reconnect timer, or durable
store. `FipsInvWantTcpDriver` is its thin `fips-tcp-endpoint` production
carrier. It advertises a caller-selected authenticated service namespace and
version, accepts or opens streams for authenticated FIPS identities, drains
split and coalesced records, and executes sends through bounded partial-write
queues. The caller drives `receive`/`poll` and owns peer selection and reconnect
timing; there is no second networking runtime or hidden outbound policy.

Both the Inv/WANT envelope and FIPS service namespaces are configurable, so a
product can preserve an already-deployed protocol while deleting its private
codec, mesh wrapper, and stream plumbing. Publications made without a stream
remain in the bounded replay cache and are announced when an explicitly chosen
peer connects or reconnects.

`FipsPeerReputation` composes FIPS's authenticated peer metrics and signed
rating events with the shared social-graph reputation policy. Its default keeps
unknown peers eligible, prioritizes observed good peers, and allows known bad
peers to be omitted. It also restores persisted rating state and exposes the
coalesced local rating events that a mesh runtime should publish.
The composed author policy is transport-neutral: applications can apply it to
relay ingress and other providers as well as FIPS frames.
Maintenance also expires bounded reputation and publication-cadence state;
the periodic FIPS snapshot creates fresh local observations, while pubsub
subscriptions and replay distribute rating events that already exist.
Wall-clock `new`, `ingest_event`, and `observe_event` methods remain convenient
for ordinary runtimes. Virtual-clock runtimes can instead use the additive
`new_at`, `ingest_event_at`, and `observe_event_at` methods with explicit Unix
seconds. Maintenance accepts milliseconds and uses that same supplied time for
pruning, publication cadence, and completed-event ingestion through
`maintenance_events` and `complete_maintenance_event`.

This crate never opens a Nostr relay socket and never falls back to one. Select
`nostr-pubsub-relay` explicitly when direct relay access is desired.

Version `0.3.x` uses the FIPS `0.4.x` endpoint API. The adapter keeps the same
bounded FSP datagram contract; the major-minor bump reflects the breaking FIPS
dependency rather than a second pubsub protocol or compatibility fallback.
The reliable stream API is additive and is not bound or advertised by
`FipsPubsubClient`; only a separately bound `FipsInvWantTcpDriver` advertises
its selected capability.

For peerfinding, configure FIPS with
`node.discovery.nostr.peerfinding_source: external` and construct a
`FipsPeerfinder`. Its `publish_local` and `refresh` methods operate only on the
`EventBus` supplied by the application, so a composed bus can use configured
relay providers, decentralized FIPS pubsub, or both without exposing any relay
list to FIPS. `ingest` and `ingest_fips_discovery_event` pass verified pubsub
events through FIPS's transport-neutral discovery validation and cache path.
`fips_discovery_retention_policy` supplies the matching app-scoped, bounded
cache policy for external peerfinding mode.
