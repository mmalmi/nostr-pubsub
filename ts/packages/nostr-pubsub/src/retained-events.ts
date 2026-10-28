import { retentionAcceptsEvent, type EventRetentionPolicy } from './filter.js';
import { eventOrder, selectStoredEvents } from './runtime-store.js';
import { verifyNostrEvent, type NostrEvent, type NostrFilter, type NostrVerifiedEvent, type QueryOptions } from './types.js';

export const MAX_RETAINED_EVENT_BYTES = 64 * 1024;
export const MAX_RETAINED_SNAPSHOT_BYTES = 16 * 1024 * 1024;
export const MAX_RETAINED_EVENTS = 4096;

/** A local snapshot of explicitly selected public events; never peer authorization. */
export class RetainedEvents {
  private events: NostrVerifiedEvent[] = [];
  constructor(private readonly policy: EventRetentionPolicy) {
    if (!Number.isSafeInteger(policy.maxEvents) || policy.maxEvents < 1 ||
      policy.maxEvents > MAX_RETAINED_EVENTS || policy.filters.length === 0)
      throw new RangeError('Retention requires filters and 1..=4096 events');
    this.policy = structuredClone(policy);
  }
  restore(events: readonly NostrEvent[]): void {
    for (const event of events) {
      try { this.insert(event); } catch { /* Persistent bytes must pass signature admission again. */ }
    }
  }
  insert(input: NostrEvent): boolean {
    const event = verifyNostrEvent(input);
    if (!retentionAcceptsEvent(this.policy, event) || !retainedEventIsCurrent(event) ||
      this.events.some(old => old.id === event.id)) return false;
    if (bytes(event) > MAX_RETAINED_EVENT_BYTES) return false;
    const key = coordinate(event);
    if (key && this.events.some(old => coordinate(old) === key && eventOrder(old, event) <= 0)) return false;
    this.events = this.events.filter(old => retainedEventIsCurrent(old) && (!key || coordinate(old) !== key));
    this.events.push(event);
    this.events.sort(eventOrder);
    this.events.length = Math.min(this.events.length, this.policy.maxEvents);
    let size = this.events.reduce((sum, item) => sum + bytes(item) + 1, 128);
    while (size > MAX_RETAINED_SNAPSHOT_BYTES) size -= bytes(this.events.pop()!) + 1;
    return this.events.some(item => item.id === event.id);
  }
  snapshot(): NostrEvent[] { return this.events.filter(retainedEventIsCurrent).map(event => structuredClone(event)); }
  query(filters: NostrFilter[], options: QueryOptions = {}): NostrVerifiedEvent[] {
    return selectStoredEvents(this.events.filter(retainedEventIsCurrent), filters, options).map(event => verifyNostrEvent(event));
  }
}
export function retainedEventIsCurrent(event: NostrEvent): boolean {
  const now = Math.floor(Date.now() / 1000);
  const expirationTag = event.tags.find(tag => tag[0] === 'expiration');
  const value = expirationTag?.[1];
  const expiration = value !== undefined && /^\d+$/.test(value) ? BigInt(value) : undefined;
  return !(event.kind >= 20000 && event.kind < 30000) && event.created_at <= now + 300 &&
    (expirationTag === undefined || (expiration !== undefined && expiration > BigInt(now) && expiration <= 18446744073709551615n));
}
function coordinate(event: NostrEvent): string | undefined {
  if (event.kind === 0 || event.kind === 3 || (event.kind >= 10000 && event.kind < 20000))
    return event.kind + ':' + event.pubkey;
  if (event.kind >= 30000 && event.kind < 40000)
    return event.kind + ':' + event.pubkey + ':' + (event.tags.find(tag => tag[0] === 'd')?.[1] ?? '');
}
function bytes(event: NostrEvent): number { return new TextEncoder().encode(JSON.stringify(event)).length; }
