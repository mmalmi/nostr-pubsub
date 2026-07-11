# Inv/want wire compatibility

Rust `InvWantCodec` and TypeScript `InvWantCodec` carry the same compact JSON
envelope. A transport selects the protocol namespace and version; the pubsub
core does not reserve either value or add stream framing.

```json
{"protocol":"iris.fips.pubsub","version":1,"message":{"type":"want","event_id":"<64 hex characters>"}}
```

The three message variants are:

- `inventory`: `event_id`, `event_kind`, `payload_bytes`, and `hop_limit`.
- `want`: `event_id`.
- `frame`: `event_id` and the complete signed Nostr `event`.

Field names and serialized field order are compatibility-sensitive because
some transports authenticate the payload bytes. Shared vectors live in
`ts/packages/nostr-pubsub/test-data/interop-vectors.json` and are executed by
both Rust and Vitest. Change the vectors and both implementations together.

The codec only validates envelope structure and bounds. `InvWantMesh` verifies
the Nostr event ID and Schnorr signature before delivery or forwarding. It also
bounds fanout, hops, event size, caches, pending peers, and route lifetimes.

Peer discovery is outside this wire format. Applications can discover signed
FIPS endpoint adverts over ordinary Nostr subscriptions, admit those peers
under product policy, connect them with FIPS, and pass the connected peer IDs
to `InvWantMesh`. No relay URL or gateway is built into this package.
