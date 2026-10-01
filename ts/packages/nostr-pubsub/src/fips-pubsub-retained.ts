import type { NostrEventReader } from './event-bus.js';
import { FipsPubsubEventCache, inventoryMessage } from './fips-pubsub-client-protocol.js';
import type { NostrEvent, NostrFilter, NostrVerifiedEvent } from './types.js';
import type { FipsPubsubWireMessage } from './wire.js';

/** Only queries application-owned retained storage; never discovers peers or fetches remotely. */
export class FipsRetainedReplay {
  private readonly pending = new Map<string, AbortController>();
  constructor(
    private readonly cache: FipsPubsubEventCache,
    private readonly reader: NostrEventReader | undefined,
    private readonly limit: number,
    private readonly maxHops: number,
    private readonly admit: (event: NostrEvent) => NostrVerifiedEvent,
    private readonly interested: (peer: string, subscription: string, event: NostrVerifiedEvent) => boolean,
    private readonly send: (peer: string, message: FipsPubsubWireMessage) => Promise<void>,
  ) {}
  async replay(peer: string, subscription: string, filters: NostrFilter[]): Promise<void> {
    const announced = new Set<string>();
    const offer = async (event: NostrVerifiedEvent, hopLimit: number): Promise<void> => {
      if (announced.size >= this.limit || announced.has(event.id) || !this.interested(peer, subscription, event)) return;
      announced.add(event.id);
      this.cache.remember(event, undefined, hopLimit);
      await this.send(peer, inventoryMessage(event, [subscription], hopLimit));
    };
    for (const cached of this.cache.replay(filters, this.limit)) await offer(cached.event, cached.hopLimit);
    if (!this.reader || announced.size >= this.limit) return;
    const key = `${peer}:${subscription}`;
    if (this.pending.has(key) || this.pending.size >= 32) return;
    const controller = new AbortController(); this.pending.set(key, controller);
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const query = this.reader.query(filters, { limit: this.limit, deadline: Date.now() + 2000, signal: controller.signal });
      const report = await Promise.race([query, new Promise<never>((_, reject) => {
        timer = setTimeout(() => { controller.abort(); reject(new Error('Retained event query timed out')); }, 2000);
      })]);
      if (controller.signal.aborted) return;
      for (const { event } of report.events.slice(0, this.limit)) {
        try { await offer(this.admit(event), this.maxHops); } catch { /* invalid/disallowed retained events are never advertised */ }
      }
    } finally {
      if (timer) clearTimeout(timer);
      this.pending.delete(key);
    }
  }
  close(): void { for (const controller of this.pending.values()) controller.abort(); this.pending.clear(); }
}
