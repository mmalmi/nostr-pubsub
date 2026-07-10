# nostr-pubsub-fips

`FipsEndpoint` provider for local-only Nostr pubsub. It uses the standard Nostr
`REQ`, `EVENT`, and `CLOSE` JSON messages on FIPS service port `7368`.

The provider fans each subscription and publication to authenticated,
connected Ethernet peers reported by FIPS. Subscription replies are accepted
only from the peers that received that subscription. Frames, peer fanout,
active subscriptions, replay deduplication, and delivery queues are bounded;
replay defaults to at most 8 events per filter.

`FipsPeerReputation` composes FIPS's authenticated peer metrics and signed
rating events with the shared social-graph reputation policy. Its default keeps
unknown peers eligible, prioritizes observed good peers, and allows known bad
peers to be omitted. It also restores persisted rating state and exposes the
coalesced local rating events that a mesh runtime should publish.
The composed author policy is transport-neutral: applications can apply it to
relay ingress and other providers as well as FIPS frames.

This crate never opens a Nostr relay socket and never falls back to one. Select
`nostr-pubsub-relay` explicitly when direct relay access is desired.
