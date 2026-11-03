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
  | { type: 'notifications'; owner: string }
  | { type: 'following'; owner: string; contacts: NostrEvent }
) & { kinds?: readonly number[] };

type Receive = (record: QueryEvent) => void;
const idle = () => ({ close() {} });
const maxAuthorsPerFilter = 256;
const maxAuthorBatches = 64;

function privateKind(kind: number): boolean {
  return kind === 4 || kind === 13 || kind === 14 || isPrivateRelayEvent({ kind });
}

export function privatePeerEvent(event: NostrEvent): boolean {
  return privateKind(event.kind) || (event.kind === 37368 && event.tags.some(tag => tag[0] === 'type' && tag[1] === 'group_roster'));
}

const notificationKinds = [1, 6, 7, 16, 20, 1111, 9735, 30023];
export function validPublicFeedFilter(filter: NostrFilter): boolean {
  const kinds = filter.kinds;
  if (!kinds?.length || kinds.length > 64 || kinds.some(privateKind)) return false;
  const keys = Object.keys(filter).filter(key => key !== 'kinds');
  if (!keys.length) return true;
  if (keys.length !== 1) return false;
  if (keys[0] === 'authors') return Boolean(filter.authors?.length && filter.authors.length <= maxAuthorsPerFilter);
  return keys[0] === '#p' && filter['#p']?.length === 1 && kinds.every(kind => notificationKinds.includes(kind));
}

function feedFilters(feed: PublicPeerFeed): NostrFilter[] {
  const kinds = [...new Set(feed.kinds ?? [1])].sort((a, b) => a - b);
  if (!kinds.length || kinds.length > 64 || kinds.some(kind => !Number.isSafeInteger(kind) || kind < 0 ||
    kind > 65535 || privateKind(kind))) {
    throw new RangeError('Choose public event kinds for the peer feed');
  }
  if (feed.type === 'notifications') {
    if (!/^[0-9a-f]{64}$/.test(feed.owner) || kinds.some(kind => !notificationKinds.includes(kind))) {
      throw new Error('Notifications require the signed-in account and public notification kinds');
    }
    return [{ kinds, '#p': [feed.owner] }];
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

/** Internal carrier binding. Only feedFilters can construct an account-lifetime subscription. */
export const publicFeedSubscribe = Symbol('publicFeedSubscribe');
export interface PublicFeedCarrier {
  [publicFeedSubscribe]?(filter: NostrFilter, receive: Receive): NostrEventSubscription;
}

async function openFilters(source: RuntimeSource, filters: NostrFilter[], receive: Receive): Promise<NostrEventSubscription> {
  const carrier = source as RuntimeSource & PublicFeedCarrier;
  if (!source.subscribe && !carrier[publicFeedSubscribe]) throw new Error('Peer feed subscription unavailable');
  let closed = false;
  const results = await Promise.allSettled(filters.map(async filter => {
    const handler = (record: QueryEvent) => {
      if (closed) return;
      const event = verifyNostrEvent(record.event);
      if (privatePeerEvent(event)) return;
      if (matchFilters([filter], event) && !expired(event)) receive({ ...record, event });
    };
    return carrier[publicFeedSubscribe]?.(filter, handler) ?? source.subscribe!([filter], handler);
  }));
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
      if (privatePeerEvent(event)) {
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

type FeedSet = PublicPeerFeed | readonly PublicPeerFeed[];
export interface PublicPeerFeedSource extends RuntimeSource {
  replaceFeed(feed: FeedSet): Promise<void>;
  close(): void;
}

/** One account-lifetime source. Overlapping feeds share a bounded local display window. */
export async function openPublicPeerFeedSource(source: RuntimeSource, feed: FeedSet): Promise<PublicPeerFeedSource> {
  type Entry = { filter: NostrFilter; handle?: NostrEventSubscription; ready?: Promise<void> };
  const scopes = new Map<string, Entry>();
  const records = new Map<string, { record: QueryEvent; scopes: Set<Entry> }>();
  const listeners = new Set<{ filters: NostrFilter[]; receive: Receive }>();
  let owner: string | undefined, closed = false;
  function retire(key: string, entry: Entry) {
    scopes.delete(key);
    entry.handle?.close();
    for (const [id, held] of records) {
      held.scopes.delete(entry);
      if (!held.scopes.size) records.delete(id);
    }
  }
  const result: PublicPeerFeedSource = {
    ...publicPeerSource(source),
    async replaceFeed(next) {
      if (closed) throw new Error('Peer feed is closed');
      const feeds: readonly PublicPeerFeed[] = Array.isArray(next) ? next : [next as PublicPeerFeed];
      if (feeds.length > 2) throw new Error('Choose at most two account feeds');
      const owners = new Set(feeds.flatMap(f => f.type === 'latest' ? [] : [f.owner]));
      if (owners.size > 1) throw new Error('Peer feeds must belong to one account');
      const nextOwner = [...owners][0];
      const selected = new Map(feeds.flatMap(feedFilters).map(filter => [JSON.stringify(filter), filter]));
      for (const [key, entry] of scopes) if (owner !== nextOwner || !selected.has(key)) retire(key, entry);
      owner = nextOwner;
      const batch = [...selected].map(([key, filter]) => {
        const active = scopes.get(key);
        if (active) return active;
        const entry: Entry = { filter };
        scopes.set(key, entry);
        entry.ready = openFilters(source, [filter], record => {
          if (scopes.get(key) !== entry) return;
          const held = records.get(record.event.id);
          if (held) { held.scopes.add(entry); return; }
          records.set(record.event.id, { record, scopes: new Set([entry]) });
          if (records.size > 256) records.delete(records.keys().next().value!);
          for (const listener of listeners) if (matchFilters(listener.filters, record.event)) listener.receive(record);
        }).then(handle => {
          if (scopes.get(key) === entry) entry.handle = handle;
          else handle.close();
        });
        return entry;
      });
      try { await Promise.all(batch.map(entry => entry.ready)); }
      catch (error) {
        for (const [key, entry] of scopes) if (batch.includes(entry)) retire(key, entry);
        throw error;
      }
    },
    async query(filters, options = {}) {
      validateQueryOptions(options);
      options.signal?.throwIfAborted();
      const events = selectStoredEvents(Array.from(records.values(), held => held.record.event), filters, options);
      return { events: events.map(event => records.get(event.id)!.record), complete: false };
    },
    subscribe(filters, receive) {
      if (closed) throw new Error('Peer feed is closed');
      const listener = { filters: filters.map(cloneFilter), receive };
      listeners.add(listener);
      return { close() { listeners.delete(listener); } };
    },
    close() { closed = true; for (const [key, entry] of scopes) retire(key, entry); listeners.clear(); },
  };
  await result.replaceFeed(feed);
  return result;
}
