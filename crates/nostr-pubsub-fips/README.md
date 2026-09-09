# nostr-pubsub-fips

Reliable authenticated Nostr pubsub over FIPS.

`FipsPubsubClient` carries normal Nostr `REQ`, `EVENT`, and `CLOSE` JSON frames
as bounded records over `fips-tcp`, using FIPS service port `7368` and
capability `nostr.pubsub/1`. There is no raw-FSP datagram fallback and this
crate opens no Nostr relay socket.

The Rust dependencies are published as `nvpn-fips-core`, `nvpn-fips-tcp`, and
`nvpn-fips-tcp-endpoint`. Their dependency aliases preserve the established
Rust API names.

Every high-level client keeps one bounded default subscription for signed
`fips-overlay-v1` kind `37195` endpoint adverts. It publishes its FIPS-generated
local advert into replay, refreshes it at half its signed TTL (capped at 30
minutes), ingests received adverts through FIPS's normal validator, and gossips
them over matching FIPS subscriptions. This works with an empty Nostr relay
list. Applications with a social-graph policy should use
`FipsPubsubClient::start_with_policy`; admission runs before local delivery,
replay retention, or forwarding.

Applications can supply known service identities in
`FipsPubsubClientOptions::routed_peers`, then replace that bounded roster using
`set_routed_peers` as authorized devices join or leave. These destinations take
priority within the connection limit and use ordinary authenticated FIPS
routing. Intermediate nodes need no pubsub service or matching subscription.
The roster provides identities, not physical addresses or a separate routing
protocol; the endpoint still needs a working route. Restricting peer transports
and routed identities cannot be combined because the endpoint API does not
expose every transport along an indirect path.
The supplied roster must fit `max_connected_peers`; temporarily unreachable
configured identities retain their slots until the application removes them.

Live mesh delivery is inventory-first. For every new event, providers send a
small `INV` containing every matching open `REQ` subscription ID for that peer.
A receiver dedupes the event ID across all peers and all of its live
subscriptions, sends one one-event `WANT` to one provider, and receives one
ordinary subscription `EVENT`. That verified event is then delivered once to
every matching local subscription.
Alternate providers are retained within fixed bounds and selected if the first
request does not complete. `CLOSE` retains its normal subscription semantics.

Recent events are kept in a bounded replay cache so a `REQ` or reconnected TCP
peer can receive inventories for the live window. The same `INV`/`WANT` flow
also works for historical events. For a large stored set, a reconciliation
layer such as NIP-77 Negentropy can identify the missing IDs first and then use
the same event transfer path more efficiently.

An explicit local publish retries a payload that has left this replay window,
while duplicate incoming gossip stays suppressed. Durable application outboxes
should pace retry batches within `max_replay_events` so payloads remain
available while peers request them; the client itself is not a durable store.

The package also exports the lower-level `FipsInvWantTcpDriver` for non-Nostr
mesh protocols, plus transport-neutral FIPS peerfinding and reputation
adapters. It targets the FIPS `0.4.x` endpoint API.
