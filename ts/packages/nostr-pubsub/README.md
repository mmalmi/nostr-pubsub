# nostr-pubsub

Use one client to read, subscribe to, and publish signed Nostr events across
the relays you choose. The runtime verifies signatures and removes duplicate
notes from query results. It also supports local event
stores and peer-to-peer sources.

## Install

```sh
npm install nostr-pubsub
```

## Read recent text notes from a relay

Save this as `read-notes.mjs`. It uses Node.js 24 and its built-in WebSocket
client. Reading public notes needs no signing key.

```js
import { NostrRuntime } from 'nostr-pubsub';

const relay = process.argv[2];
if (!relay) throw new Error('Pass a relay URL as the first argument.');

const runtime = new NostrRuntime({ relays: [relay] });
try {
  const { events, complete } = await runtime.query(
    [{ kinds: [1], limit: 5 }],
    { deadline: Date.now() + 5000 },
  );

  for (const note of events) console.log(note.content);
  if (!complete) console.error('Relay history is incomplete.');
} finally {
  await runtime.close();
}
```

Run it with the URL of a relay that serves public notes.

```sh
node read-notes.mjs wss://relay.damus.io
```

Kind 1 is a short text note. The query prints up to five notes, newest first.
If the relay does not finish within five seconds, the example prints any notes
received and reports incomplete history.

## More

The [TypeScript guide](https://github.com/mmalmi/nostr-pubsub/blob/nostr-pubsub-ts-v0.5.41/docs/typescript-guide.md)
covers live subscriptions, publishing and retries, caching, pagination, private
messages, and peer-to-peer routing. The [changelog](https://github.com/mmalmi/nostr-pubsub/blob/nostr-pubsub-ts-v0.5.41/ts/packages/nostr-pubsub/CHANGELOG.md)
lists changes by version.
