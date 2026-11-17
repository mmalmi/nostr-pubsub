import type { NostrEvent } from './types.js';
import type { RuntimeEventStore, RuntimeOutboxEntry, RuntimePendingScope, RuntimePublishOptions } from './runtime-types.js';

type Destination = Pick<RuntimePublishOptions, 'relays' | 'sources'>;
type Update = (current: RuntimeOutboxEntry | undefined) => RuntimeOutboxEntry | undefined;
const writers = new WeakMap<RuntimeEventStore, Promise<void>>();

/** Older stores key complete records by event ID. Serialize their read/modify/write across runtime owners. */
export function updatePending(store: RuntimeEventStore, id: string, update: Update): Promise<void> {
  if (store.updatePending) return store.updatePending(id, update);
  const operation = (writers.get(store) ?? Promise.resolve()).then(async () => {
    const current = (await store.listPending()).find(row => row.event.id === id);
    const next = update(current);
    if (next === current) return;
    if (!next) {
      if (current) await store.deletePending(id);
      return;
    }
    await store.putPending(next);
    if (next.additionalScopes?.length) {
      // Some older adapters persist only the original columns. Never report those new sends as queued.
      const saved = (await store.listPending()).find(row => row.event.id === id);
      const scopes = saved?.additionalScopes ?? [];
      if (scopes.length !== next.additionalScopes.length ||
          !scopes.every((scope, index) => scopeKey(scope) === scopeKey(next.additionalScopes![index]!))) {
        throw new Error('Pending store must retain additionalScopes for independent destinations');
      }
    }
  });
  writers.set(store, operation.catch(() => {}));
  return operation;
}

/** Capture caller-owned selections before asynchronous persistence or delivery. */
export function publicationOptions(options: RuntimePublishOptions): RuntimePublishOptions {
  return { ...options, relays: options.relays?.slice(), sources: options.sources?.slice() };
}

function scopeKey({ relays, sources }: Destination): string {
  // Omitted destinations follow runtime configuration. An empty selection is explicit.
  return JSON.stringify([relays && [...new Set(relays)].sort(), sources && [...new Set(sources)].sort()]);
}

export function pendingPublications(rows: RuntimeOutboxEntry[]): RuntimeOutboxEntry[] {
  return rows.flatMap(({ event, additionalScopes = [], ...first }) =>
    [first, ...additionalScopes].map(scope => ({ ...scope, event })));
}

export function updatePublicationScope(store: RuntimeEventStore, event: NostrEvent, destination: Destination, action: 'queue' | 'retire'): Promise<void> {
  const key = scopeKey(destination);
  return updatePending(store, event.id, current => {
    const scopes: RuntimePendingScope[] = [];
    if (current) {
      const { event: _event, additionalScopes = [], ...first } = current;
      scopes.push(first, ...additionalScopes);
    }
    const index = scopes.findIndex(scope => scopeKey(scope) === key);
    if (action === 'retire') {
      if (index < 0) return current;
      scopes.splice(index, 1);
    } else {
      const { relays, sources } = destination;
      const next = { relays, sources, updatedAt: Date.now(), attempts: (scopes[index]?.attempts ?? 0) + 1 };
      if (index < 0) scopes.push(next);
      else scopes[index] = next;
    }
    const [first, ...additionalScopes] = scopes;
    return first && { event, ...first, ...(additionalScopes.length ? { additionalScopes } : {}) };
  });
}
