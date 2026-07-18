# nostr-pubsub-fips

Reliable authenticated Nostr pubsub over FIPS.

`FipsPubsubClient` carries normal Nostr `REQ`, `EVENT`, and `CLOSE` JSON frames
as bounded records over `fips-tcp`, using FIPS service port `7368` and
capability `nostr.pubsub/1`. There is no raw-FSP datagram fallback and this
crate opens no Nostr relay socket.

Live mesh delivery is inventory-first. For every new event, providers send a
small `INV` containing every matching open `REQ` subscription ID for that peer.
A receiver dedupes the event ID across all peers and all of its live
subscriptions, sends one one-event `WANT` to one provider, and receives one
ordinary subscription `EVENT`. That verified event is then delivered once to
every matching local subscription.
Alternate providers are retained within fixed bounds and selected if the first
request does not complete. `CLOSE` retains its normal subscription semantics.

Recent events are kept in a bounded replay cache so a `REQ` or reconnected TCP
peer can receive inventories for the live window. Bulk historical set
reconciliation is intentionally separate (for example NIP-77 Negentropy); it
is not conflated with live duplicate suppression.

The package also exports the lower-level `FipsInvWantTcpDriver` for non-Nostr
mesh protocols, plus transport-neutral FIPS peerfinding and reputation
adapters. It targets the FIPS `0.4.x` endpoint API.
