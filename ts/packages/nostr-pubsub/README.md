# nostr-pubsub for TypeScript

Browser-ready Nostr source routing, bounded peer subscriptions, FIPS Nostr
frames, and signed-event inventory/want propagation. The TypeScript package
lives beside the canonical Rust crates and shares their interoperability
vectors.

```ts
import { InvWantCodec, InvWantMesh, meshPeer } from 'nostr-pubsub';

const codec = new InvWantCodec('iris.fips.pubsub', 1, 64 * 1024);
const mesh = new InvWantMesh({ allowedKinds: new Set([37_195]) });

for (const action of mesh.publish(signedEvent, [meshPeer(remoteFipsPubkey)], Date.now())) {
  if (action.type === 'send') {
    await fips.send(action.peerId, codec.encode(action.message));
  }
}
```

The application owns the relay URL list, peer-advert meaning, FIPS peer
admission, and outbound connection policy. `SimplePoolNostrRelayTransport`
provides the browser/WebSocket relay mechanics without choosing a default relay
or gateway. See the repository `docs/inv-want-wire.md` for compatibility and
security boundaries.

## Shared application runtime

Enable optional NIP-77 gap recovery with `reconciliation: {}` in the runtime
options. Cached events and ordinary live `REQ` start immediately. After NIP-11
advertises support, a bounded comparison runs on the same relay connection;
missing IDs are fetched by ordinary `REQ` and use the same signature, deletion,
expiration and replacement admission rules. It never uploads local-only events.

The default comparison covers at most 24 hours ending at the original filter's
`until` (or now). An explicitly requested older page stays in that older time
range. Original relay filters and their history completion remain unchanged:
this is best-effort gap recovery for a slice, not an all-history sync claim.
ID-only reads, cache-only reads and subscriptions without relays skip it.
Unsupported relays, malformed frames, inventory limits and timeouts leave normal
subscriptions running. Defaults limit the snapshot to 2048 records, missing IDs
to 256, frames to 8192 bytes, rounds to 8 and total work to 3 seconds. At most two
comparisons run at once, with one per relay and a five-minute relay/filter cooldown.
`RuntimeReconciliationOptions` exposes these bounds and an optional NIP-11 fetch
adapter; reconciliation is disabled unless explicitly configured.

`NostrRuntime` owns one relay pool, batches concurrent interests as exact OR
filters, and composes additional `RuntimeSource` implementations such as
`FipsNostrPubsubEventSource`. Filters retain their author/recipient intersections;
filter count, encoded bytes, active subscriptions, verification cache, and
per-subscription duplicate tracking are bounded. Run it in a worker when the
application already owns a worker; forward operations instead of starting a
second pool in the UI.

```ts
import { NostrRuntime, FipsNostrPubsubEventSource } from 'nostr-pubsub';

const runtime = new NostrRuntime({
  relays: configuredRelays,
  store: applicationEventStore,
  sources: [new FipsNostrPubsubEventSource(pubsub)],
});
const subscription = runtime.subscribe([{ kinds: [1], authors }], {
  onEvent: (event, info) => renderEvent(event, info.cached),
  onEose: (status) => showHistoryStatus(status.complete),
});
const result = await runtime.query([{ kinds: [0], authors }], {
  cache: 'cache-first', deadline: Date.now() + 2000, signal,
});
const publication = await runtime.publish(signedEvent);
// Confirmed state changes can require remote acceptance before local delivery.
await runtime.publish(signedUpdate, { requireAck: true });
subscription.close();
await runtime.close();
```

`RuntimeEventStore` is storage only: query/put/delete events and list/put/delete
pending publications. Supply a Hashtree index or IndexedDB adapter to retain
history and outbox intent across reloads. The bounded default `MemoryEventStore`
is volatile. No login keys or signer state are stored by the runtime, and no old
application event cache is migrated. The runtime verifies events and applies
replaceable-event ordering, expiration, and author-authorized deletion tombstones.

Only actual relay EOSE is complete. A deadline, disconnected source, or partial
mesh history reports incomplete and leaves live subscriptions running. Relay
reconnect overlaps the last seen second and deduplicates event IDs. `cache-only`
queries complete from local storage; `network-only` omits initial cache replay.
Explicit `relays` scope also excludes additional sources unless `sources` is
specified, and the outbox preserves those destinations when retrying.

`publish().remoteAccepted` requires a positive relay/source acknowledgment.
Local echo, persisted retry intent, and a FIPS send queue do not count as remote
acceptance. `requireAck: true` disables both pre-acceptance local delivery and
queueing; the first positive acknowledgment completes without waiting for silent
relays. Optional `signAuthEvent(relay, template)` handles relay authentication
using the application's existing signer.

Give `FipsNostrPubsubClient` a `retainedEventReader` to serve its local event
index through the same FIPS node used by Hashtree files. Queries are bounded by
the configured replay count, two seconds, and 32 concurrent requests. The reader
must be local-only; incoming peer requests must not trigger network fetches.
Existing peer admission and kind policy still apply. This hook neither discovers
peers nor changes application sharing permissions.

## Event readers and dataset routing

`NostrEventReader`, `NostrEventPublisher`, and `NostrEventSubscriber` let
applications compose read-only indexes and live sources without granting them
publication authority. `EventBus` combines the reader and publisher contracts.
Routes with one `datasetId`
are replicas of the same logical data; different dataset identities are
additive and their verified events are merged:

```ts
const report = await queryRoutesWithPolicy([
  {
    route: withRouteDataset(localIndexRoute('archive-primary'), 'archive'),
    reader: archivePrimary,
  },
  {
    route: withRouteDataset(localIndexRoute('archive-replica'), 'archive'),
    reader: archiveReplica,
  },
  {
    route: withRouteDataset(localIndexRoute('personal'), 'personal'),
    reader: personalIndex,
  },
], filters, {
  query: { limit: 100, signal, deadline: Date.now() + 2_000 },
}, policy);
```

The historical router tries same-dataset replicas in policy order until one
produces a complete response, queries allowed additive datasets concurrently,
isolates source failures, and applies one newest-first limit after verified-ID
deduplication. The live router opens every allowed source, globally deduplicates
event IDs across noisy mesh/index/relay subscriptions, and closes every source
as one subscription. Reports retain route provenance and dataset completeness;
there is no legacy `{ route, bus }` alias.

CPU-heavy signature checks can run behind an asynchronous trust boundary such
as a dedicated Web Worker. `verifyNostrEventsWith()` gives the verifier
immutable defensive clones, requires one boolean result per event, and admits
fresh canonical copies only when the whole batch is valid:

```ts
const verified = await verifyNostrEventsWith(events, workerVerifier, { signal });
```

The router recognizes those returned copies and does not repeat Schnorr
verification. Keep the injected verifier private to the trusted worker
request/response path; an all-true verifier is not a general-purpose shortcut.

`NostrPubsubRouter` owns those explicit query, publish, and live route lists for
long-running services. A Hashtree index can be query-only, while FIPS and relay
adapters can independently participate in publication and live subscription;
the router never invents a fallback backend.

The standard relay adapter reads the application's relay list at operation
start, verifies every inbound and outbound event at its boundary, deduplicates
relay URLs, and accepts publication when at least one relay succeeds. Historical
queries finish on aggregate EOSE or a configurable inactivity window; ordinary
live subscriptions remain open:

```ts
import {
  NostrRelayEventSource,
  SimplePoolNostrRelayTransport,
} from 'nostr-pubsub';

const relayTransport = new SimplePoolNostrRelayTransport({
  getRelays: () => settings.readRelays(),
  queryQuietWindowMs: 600,
});
const relayEvents = new NostrRelayEventSource('configured-relays', relayTransport);

const historical = await relayEvents.query([{ kinds: [0], authors }], {
  deadline: Date.now() + 2_000,
});
const live = relayEvents.subscribe([{ kinds: [1], since }], receiveEvent);
```

`FipsNostrPubsubClient` is the browser peer carrier matching Rust
`FipsPubsubClient` on authenticated reliable FIPS-TCP service
`nostr.pubsub/1` (port 7368). It carries verified Nostr `REQ`, `EVENT`, and
`CLOSE` frames plus grouped `INV` and one-event `WANT`. Historical catch-up and
new live events use this same flow. The application supplies admitted peer
identities explicitly, so arbitrary connected FIPS peers are never treated as
pubsub providers:

```ts
import { FipsNostrPubsubClient } from 'nostr-pubsub';

const pubsub = new FipsNostrPubsubClient({
  node: fipsNode,
  localPeerId: fipsNode.identity.publicKey,
  peers: () => appOwnedStandaloneLinks.map((link) => link.remotePubkey),
  allowedKinds: [1059, 1060, 30078, 37368],
}).start();

const subscription = pubsub.subscribe([{ kinds: [1060] }], (event) => {
  receiveSignedChatEvent(event);
});
await pubsub.publish(signedChatEvent);
subscription.close();
```

Records are bounded to the shared TCP/FIPS maximum of 65,525 bytes.
Peer refresh events can add or restore explicitly admitted standalone links;
they do not create links or infer admission policy.

For reliable authenticated carriage, `FipsInvWantStream` applies the same
four-byte big-endian record framing and bounds as Rust. `FipsInvWantTcpDriver`
binds that stream to `@fips/tcp`, accepts only the peer identity supplied by the
authenticated FSP service context, converges simultaneous connects on one
stream, and owns bounded partial-read/write queues. Applications explicitly
choose peers and reconnect timing:

```ts
import { FipsInvWantStream, FipsInvWantTcpDriver } from 'nostr-pubsub';

const stream = new FipsInvWantStream();
const driver = FipsInvWantTcpDriver.bind(
  fipsNode,
  localFipsPubkey,
  stream,
  {
    serviceNamespace: 'nostr.pubsub',
    serviceVersion: 1,
    servicePort: 39_121,
    maxPeers: 64,
    maxQueuedRecordsPerPeer: 64,
    maxQueuedBytesPerPeer: 2 * 1024 * 1024,
    maxIoBytesPerDrive: 256 * 1024,
  },
);
await driver.connectPeer(remoteFipsPubkey);
const report = await driver.poll();
```

`fipsInvWantTcpCapabilityName()` returns the authenticated capability name for
the configured namespace and version. Capability roster registration remains
an FSP concern; the TypeScript FIPS API does not yet expose the Rust endpoint's
lifecycle-bound capability registration. The driver does not advertise through
plaintext discovery or add a product-local fallback. The old
`FipsNostrRelayService` datagram bridge has been removed; use
`FipsNostrPubsubClient` and the router adapters.

The simultaneous-connect tie-break normalizes FIPS's compressed hex peer keys
to NIP-19 `npub` strings before ordering them. This deliberately matches the
Rust driver's ordering; comparing the raw compressed hex would sometimes make
the two runtimes select and reset opposite streams.

`InvWantMesh` matches the Rust production state machine: inventories use
canonical lowercase event IDs and local kind, size, and hop bounds; repeated
inventories must keep identical event kind and size, while remaining hop
budgets may differ by path under the local cap; and recovery uses no more than
three providers. Frames are accepted only from a provider sent a `WANT` and
only when the verified signature, ID, kind, and serialized size match the
inventory. Bounded fulfilled-route provenance absorbs delayed valid answers
from requested alternatives without scoring them, while unrequested sources
remain invalid. A want with neither a cached event nor a live route is ignored,
and related transient state expires or is evicted together. Cached payloads are
bounded by both count and `maxCachedEventBytes`, whose aggregate default is
16 MiB. Seen-inventory and delivered-event deduplication have both TTL and count
bounds. `retainedState()` reports raw cache bytes and state counts, while
`maintain()` and `peerBehaviorObservation()` expose the same maintenance and
evidence semantics as Rust.

If a local transport confirms that the stream or link carrying one active
request failed, `recordTransportDisruption()` marks only that peer/event
attempt so expiry does not falsely blame the provider. Do not derive this
signal from peer-supplied data. Its bounded mark clears on completion, expiry,
or a new `WANT` attempt to that peer.

`publishVerified()`, `replayVerifiedToPeer()`, and `receiveVerifiedFrame()`
avoid a repeated signature check only for immutable events returned by
`verifyNostrEvent()`. Use `publish()`, `replayToPeer()`, or `receive()` for
untrusted input. Type assertions are not a trust boundary and are rejected by
the verified fast paths.
