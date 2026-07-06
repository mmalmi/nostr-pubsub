# nostr-pubsub-relay

Optional actual-relay backend for `nostr-pubsub`.

`RelayEventBus` adapts `nostr-sdk` relay connections to the core
`nostr_pubsub::EventBus` trait. Use it when an application wants normal Nostr
relays as one source in the pubsub routing graph, usually after local indexes
and direct peer transports.
