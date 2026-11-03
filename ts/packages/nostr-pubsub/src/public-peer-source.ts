import { matchFilters } from 'nostr-tools/filter';
import type { NostrEventSubscription, QueryEvent } from './event-bus.js';
import { cloneFilter } from './filter.js';
import { isPrivateRelayEvent } from './private-relay-publisher.js';
import { expired, selectStoredEvents } from './runtime-store.js';
import type { EventSource } from './source.js';
import type { RuntimeSource } from './runtime-types.js';
import { validateQueryOptions, verifyNostrEvent, type NostrEvent, type NostrFilter } from './types.js';

export type PublicPeerFeed = (
  | { type: 'latest' }
  | { type: 'following'; owner: string; contacts: NostrEvent }
) & { kinds?: readonly number[] };

type Receive = (record: QueryEvent) => void;
const idle = () => ({ close() {} });
const maxAuthorsPerFilter = 256;
const maxAuthorBatches = 64;

function privateKind(kind: number): boolean {
  return kind === 4 || kind === 13 || kind === 14 || isPrivateRelayEvent({ kind });
}

function privateEvent(event: NostrEvent): boolean {
  return privateKind(event.kind) || (event.kind === 37368 && event.tags.some(tag => tag[0] === 'type' && tag[1] === 'group_roster'));
}

function feedFilters(feed: PublicPeerFeed): NostrFilter[] {
  const kinds = [...new Set(feed.kinds ?? [1])].sort((a, b) => a - b);
  if (!kinds.length || kinds.length > 64 || kinds.some(kind => !Number.isSafeInteger(kind) || kind < 0 ||
    kind > 65535 || privateKind(kind))) {
    throw new RangeError('Choose public event kinds for the peer feed');
  }
  if (feed.type === 'latest') return [{ kinds }];
  if (feed.type !== 'following') throw new Error('Unknown peer feed');
  const contacts = verifyNostrEvent(feed.contacts);
  if (contacts.kind !== 3 || contacts.pubkey !== feed.owner || expired(contacts)) throw new Error('Expected the owner’s signed public follows');
  const authors = [...new Set(contacts.tags.filter(tag => tag[0] === 'p' && /^[0-9a-f]{64}$/.test(tag[1] ?? ''))
    .map(tag => tag[1]!))].sort();
  if (authors.length > maxAuthorsPerFilter * maxAuthorBatches) throw new RangeError('Public follow list exceeds peer feed capacity');
  const filters: NostrFilter[] = [];
  for (let i = 0; i < authors.length; i += maxAuthorsPerFilter) {
    filters.push({ kinds, authors: authors.slice(i, i + maxAuthorsPerFilter) });
  }
  return filters;
}

/** A fixed background feed. Do not derive its configuration or lifetime from the open page. */
export async function subscribePublicPeerFeed(source: RuntimeSource, feed: PublicPeerFeed,
  receive: Receive): Promise<NostrEventSubscription> {
  return openFilters(source, feedFilters(feed), receive);
}

async function openFilters(source: RuntimeSource, filters: NostrFilter[], receive: Receive): Promise<NostrEventSubscription> {
  if (!source.subscribe) throw new Error('Peer feed subscription unavailable');
  let closed = false;
  const results = await Promise.allSettled(filters.map(async filter => source.subscribe!([filter], record => {
    if (closed) return;
    const event = verifyNostrEvent(record.event);
    if (privateEvent(event)) return;
    if (matchFilters([filter], event) && !expired(event)) receive({ ...record, event });
  })));
  const handles = results.flatMap(result => result.status === 'fulfilled' ? [result.value] : []);
  const close = () => {
    closed = true;
    for (const handle of handles.splice(0)) handle.close();
  };
  const failure = results.find(result => result.status === 'rejected');
  if (failure) { close(); throw failure.reason; }
  return { close };
}

/** Application queries have no peer history of their own. Explicit publication keeps its original receipt. */
export function publicPeerSource(source: RuntimeSource): RuntimeSource {
  return {
    id: source.id, publishAcceptance: source.publishAcceptance,
    ...(source.publish && { async publish(input: NostrEvent, origin: EventSource) {
      const event = verifyNostrEvent(input);
      if (privateEvent(event)) {
        throw new Error('Private event cannot be published on a public peer feed');
      }
      return source.publish!(event, origin);
    } }),
    async query(_filters, options = {}) {
      validateQueryOptions(options);
      options.signal?.throwIfAborted();
      return { events: [], complete: false };
    },
    subscribe: idle,
  };
}

export interface PublicPeerFeedSource extends RuntimeSource {
  replaceFeed(feed: PublicPeerFeed): Promise<void>;
  close(): void;
}

/** One account-lifetime peer feed. All application filters match the bounded feed window locally. */
export async function openPublicPeerFeedSource(source: RuntimeSource, feed: PublicPeerFeed): Promise<PublicPeerFeedSource> {
  const records = new Map<string, QueryEvent>();
  const listeners = new Set<{ filters: NostrFilter[]; receive: Receive }>();
  let subscription: NostrEventSubscription | undefined;
  let generation = 0;
  let closed = false;
  function retire() {
    generation++;
    subscription?.close();
    subscription = undefined;
    records.clear();
  }
  const result: PublicPeerFeedSource = {
    ...publicPeerSource(source),
    async replaceFeed(next) {
      if (closed) throw new Error('Peer feed is closed');
      retire();
      const version = generation;
      const handle = await subscribePublicPeerFeed(source, next, record => {
        if (generation !== version || records.has(record.event.id)) return;
        records.set(record.event.id, record);
        if (records.size > 256) records.delete(records.keys().next().value!);
        for (const listener of listeners) if (matchFilters(listener.filters, record.event)) listener.receive(record);
      }).catch(error => {
        if (generation === version) retire();
        throw error;
      });
      if (generation === version) subscription = handle;
      else handle.close();
    },
    async query(filters, options = {}) {
      validateQueryOptions(options);
      options.signal?.throwIfAborted();
      const events = selectStoredEvents(Array.from(records.values(), record => record.event), filters, options);
      return { events: events.map(event => records.get(event.id)!), complete: false };
    },
    subscribe(filters, receive) {
      if (closed) throw new Error('Peer feed is closed');
      const listener = { filters: filters.map(cloneFilter), receive };
      listeners.add(listener);
      return { close() { listeners.delete(listener); } };
    },
    close() { closed = true; retire(); listeners.clear(); },
  };
  await result.replaceFeed(feed);
  return result;
}
