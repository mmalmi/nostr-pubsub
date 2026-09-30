# nostr-pubsub-relay

Optional actual-relay backend for `nostr-pubsub`.

`RelayEventBus` adapts `nostr-sdk` relay connections to the core
`nostr_pubsub::EventBus` trait. Use it when an application wants normal Nostr
relays as one source in the pubsub routing graph, usually after local indexes
and direct peer transports.

Live subscriptions recover known SDK notification gaps by replaying their
original filters on the configured relays, including their time bounds and
limits. Replays preserve duplicate callback delivery and the client's signer.
One pending replay is coalesced; repeated gaps or failed setup back off from
250 ms to 30 s. Each filter's setup uses the bus query timeout. Recovery adds
no event-body queue, but setup briefly pauses delivery and sustained overload
can still lose events. Ephemeral, expired, and no-longer-retained events cannot
be recovered. Closing a subscription stops recovery before sending its final
`CLOSE`.

The ordinary `NostrEventSubscriber` callback has no acceptance feedback.
Direct consumers with bounded ingress can use
`RelayEventBus::subscribe_with_admission`: return `false` for a transient full
queue to request the same coalesced replay, and `true` for accepted or handled
events. Acceptance does not mean durable storage. Closed queues and intentional
policy rejection should return `true`, since replay cannot repair them. This
does not change routed subscriptions: their deduplication runs before the user
callback, so replay alone cannot repair downstream rejection through that API.
