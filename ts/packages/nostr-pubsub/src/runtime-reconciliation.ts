import type { AbstractRelay, Subscription } from 'nostr-tools/abstract-relay';
import { matchFilters } from 'nostr-tools/filter';
import { Reconciliation } from 'nostr-pubsub-reconcile';
import type { NostrEvent, NostrFilter } from './types.js';
import type { RuntimeEventStore, RuntimeReconciliationOptions } from './runtime-types.js';
import { RelayReconciliationCapabilities } from './runtime-relay-capabilities.js';

/** Uses the pool's public custom-message subscriptions; never owns a second socket. */
export class RuntimeReconciliation {
  private readonly capabilities: RelayReconciliationCapabilities;
  private readonly active = new Map<string, () => void>();
  private readonly attempts = new Map<string, number>();
  private readonly limits;
  private serial = 0;
  constructor(options: RuntimeReconciliationOptions, private readonly store: RuntimeEventStore) {
    this.limits = {
      lookback: bound(options.lookbackSeconds, 86400, 1, 604800),
      timeout: bound(options.timeoutMs, 3000, 1, 30000),
      cooldown: bound(options.cooldownMs, 300000, 1, 86400000),
      concurrent: bound(options.maxConcurrent, 2, 1, 16),
      maxRecords: bound(options.maxRecords, 2048, 1, 100000),
      maxNeedIds: bound(options.maxNeedIds, 256, 1, 2048),
      maxFrameBytes: bound(options.maxFrameBytes, 8192, 4096, 65536),
      maxRounds: bound(options.maxRounds, 8, 1, 64),
    };
    this.capabilities = new RelayReconciliationCapabilities(options.fetch ?? globalThis.fetch?.bind(globalThis), bound(options.capabilityTimeoutMs, 1000, 1, 10000));
  }
  start(relay: AbstractRelay, originals: NostrFilter[], deliver: (event: NostrEvent) => void): () => void {
    // One exchange per connection, a fixed global capacity, and no retry timers.
    if (this.active.has(relay.url) || this.active.size >= this.limits.concurrent) return () => {};
    const now = Date.now();
    for (const [key, expires] of this.attempts) if (expires <= now) this.attempts.delete(key);
    if (this.attempts.size >= 256) return () => {};
    for (const original of originals) {
      const filter = boundedFilter(original, this.limits.lookback);
      if (!filter) continue;
      const key = relay.url + JSON.stringify(Object.entries(original).filter(([name]) => name !== 'limit').sort(([a], [b]) => a.localeCompare(b)));
      if (this.attempts.has(key)) continue;
      this.attempts.set(key, now + this.limits.cooldown);
      const controller = new AbortController();
      let control: Subscription | undefined, download: Subscription | undefined, opened = false;
      const closeControl = (): void => {
        if (!control) return;
        const previous = control; control = undefined;
        previous.onclose = undefined;
        if (opened && relay.connected) void relay.send(JSON.stringify(['NEG-CLOSE', previous.id])).catch(() => undefined);
        if (relay.openSubs.has(previous.id)) previous.close();
      };
      const close = (): void => {
        if (controller.signal.aborted) return;
        controller.abort(); clearTimeout(timer);
        this.active.delete(relay.url);
        closeControl();
        if (download && relay.openSubs.has(download.id)) download.close();
      };
      const timer = setTimeout(close, this.limits.timeout);
      this.active.set(relay.url, close);
      const current = (): boolean => !controller.signal.aborted && relay.connected;
      const run = async (): Promise<void> => {
        if (!await this.capabilities.supports(relay.url, controller.signal) || !current()) { close(); return; }
        // Override the user-facing page limit only inside this bounded inventory.
        const records = await this.store.query([{ ...filter, limit: this.limits.maxRecords + 1 }], {
          limit: this.limits.maxRecords + 1, signal: controller.signal, deadline: now + this.limits.timeout,
        });
        if (!current()) return;
        if (records.length > this.limits.maxRecords) { close(); return; }
        const engine = new Reconciliation(records.filter(event => matchFilters([filter], event)).map(event => ({
          timestamp: BigInt(event.created_at), id: fromHex(event.id, 32),
        })), { since: BigInt(filter.since!), until: BigInt(filter.until!) }, this.limits);
        const initial = await engine.initiate();
        if (!current()) return;
        const needed = new Set<string>();
        let busy = false;
        const recover = async (): Promise<void> => {
          closeControl();
          if (!needed.size) { close(); return; }
          const requested = { ...filter, ids: [...needed], limit: needed.size };
          download = relay.prepareSubscription([requested], {
            id: `pubsub-neg-fetch-${++this.serial}`,
            onevent: event => { if (current() && needed.has(event.id) && matchFilters([filter], event)) deliver(event); },
            oneose: close, onclose: close,
          });
          relay.ongoingOperations++; relay.idleSince = undefined;
          await relay.send(JSON.stringify(['REQ', download.id, requested]));
        };
        control = relay.prepareSubscription([{ ids: [] }], { id: `pubsub-neg-${++this.serial}`, onclose: close });
        relay.ongoingOperations++; relay.idleSince = undefined;
        control.oncustom = (frame) => {
          if (!current()) return;
          if (frame[0] === 'NEG-ERR') { close(); return; }
          // Never queue unbounded asynchronous hash work from an unsolicited burst.
          if (frame[0] !== 'NEG-MSG' || frame.length !== 3 || busy) { close(); return; }
          busy = true;
          void (async () => {
            const reply = fromHex(frame[2], undefined, this.limits.maxFrameBytes);
            const step = await engine.reconcile(reply);
            if (!current()) return;
            for (const id of step.need) {
              needed.add(toHex(id));
              if (needed.size > this.limits.maxNeedIds) { close(); return; }
            }
            busy = false;
            if (step.next) await relay.send(JSON.stringify(['NEG-MSG', control!.id, toHex(step.next)]));
            else await recover();
          })().catch(close);
        };
        opened = true;
        await relay.send(JSON.stringify(['NEG-OPEN', control.id, filter, toHex(initial)]));
      };
      void run().catch(close);
      return close;
    }
    return () => {};
  }
  close(): void { for (const close of [...this.active.values()]) close(); }
}

function boundedFilter(original: NostrFilter, lookback: number): NostrFilter | undefined {
  // ID reads are already exact; unknown extensions may have matching semantics the
  // local store cannot reproduce, so leave them entirely to ordinary REQ.
  if (original.ids || Object.entries(original).some(([key, value]) =>
    (!['authors', 'kinds', 'since', 'until', 'limit'].includes(key) && !key.startsWith('#')) || (Array.isArray(value) && !value.length))) return;
  const until = Math.min(original.until ?? Math.floor(Date.now() / 1000), Math.floor(Date.now() / 1000));
  const since = Math.max(original.since ?? 0, until - lookback, 0);
  if (!Number.isSafeInteger(since) || !Number.isSafeInteger(until) || since > until) return;
  const filter = { ...structuredClone(original), since, until };
  delete filter.limit;
  return filter;
}
function bound(value: number | undefined, fallback: number, min: number, max: number): number {
  const result = value ?? fallback;
  if (!Number.isSafeInteger(result) || result < min || result > max) throw new RangeError('Invalid reconciliation bounds');
  return result;
}
function fromHex(value: unknown, exactBytes?: number, maxBytes = 32): Uint8Array {
  if (typeof value !== 'string' || !value.length || value.length % 2 || value.length > maxBytes * 2 || (exactBytes !== undefined && value.length !== exactBytes * 2) || !/^[0-9a-f]+$/i.test(value)) throw new Error('Invalid reconciliation frame');
  return Uint8Array.from(value.match(/../g)!, byte => parseInt(byte, 16));
}
function toHex(value: Uint8Array): string { return Array.from(value, byte => byte.toString(16).padStart(2, '0')).join(''); }
