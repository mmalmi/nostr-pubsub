import type { EventBus } from './event-bus.js';
import type { NostrRuntime } from './runtime.js';
import { localIndexSource } from './source.js';
import { subscriptionFiltersMatch } from './filter.js';
import { retainedEventIsCurrent } from './retained-events.js';
import { verifyNostrEvent, type NostrEvent, type NostrFilter } from './types.js';

/** Public announcement bridge for a transport's injected discovery provider.
 * The caller owns persistence and network routing. Private discovery caches
 * must remain local and must never be supplied to a public replay reader.
 */
export class RetainedPeerDiscovery {
  private tail: Promise<void> = Promise.resolve();
  private pending = 0;
  private closed = false;
  private readonly subscriptions = new Set<() => void>();
  constructor(
    private readonly runtime: Pick<NostrRuntime, 'publish' | 'subscribe'>,
    readonly store: EventBus,
    private readonly onError: (error: Error) => void,
  ) {}
  async publish(input: NostrEvent): Promise<void> {
    if (this.closed) throw new Error('Peer discovery is closed');
    const event = verifyNostrEvent(input);
    if (event.kind !== 37195 || !retainedEventIsCurrent(event))
      throw new Error('Invalid public peer announcement');
    await this.store.publish(event, localIndexSource('local-peer-announcement'));
    // Persist before network delivery: unreachable carriers cannot prevent
    // local startup. The application runtime owns network delivery/retries.
    try { await this.runtime.publish(event); }
    catch (error) { this.onError(asError(error)); }
  }
  async subscribe(filter: NostrFilter,
    onEvent: (event: NostrEvent, source: string) => void): Promise<() => void> {
    if (this.closed) throw new Error('Peer discovery is closed');
    if (filter.kinds?.length !== 1 || filter.kinds[0] !== 37195)
      throw new Error('Peer discovery accepts only announcement subscriptions');
    let active = true;
    const seen = new Set<string>();
    const deliver = (input: NostrEvent, source: string) => {
      if (!active || this.closed) return;
      const event = verifyNostrEvent(input);
      if (!retainedEventIsCurrent(event) || !subscriptionFiltersMatch([filter], event) || seen.has(event.id)) return;
      seen.add(event.id);
      if (seen.size > 4096) seen.delete(seen.values().next().value!);
      onEvent(event, source);
    };
    const subscription = this.runtime.subscribe([filter], {
      onEvent: event => {
        if (!active || this.closed) return;
        if (this.pending >= 64) { this.onError(new Error('Peer announcement storage queue is full')); return; }
        this.pending++;
        this.tail = this.tail.then(async () => {
          const report = await this.store.publish(event, localIndexSource('peer-announcements'));
          if (report.accepted) deliver(event, 'pubsub');
        }).catch(error => this.onError(asError(error))).finally(() => { this.pending--; });
      },
      onError: this.onError,
    }, { cache: 'network-only' });
    const close = () => { active = false; subscription.close(); this.subscriptions.delete(close); };
    this.subscriptions.add(close);
    try {
      const saved = await this.store.query([filter], { limit: 256 });
      for (const { event } of saved.events) deliver(event, 'local-cache');
    } catch (error) { close(); throw error; }
    return close;
  }
  async close(): Promise<void> {
    this.closed = true;
    for (const close of [...this.subscriptions]) close();
    await this.tail;
  }
}
function asError(error: unknown): Error { return error instanceof Error ? error : new Error(String(error)); }
