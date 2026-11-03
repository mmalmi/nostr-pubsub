import { Reconciliation } from 'nostr-pubsub-reconcile';
import { subscriptionFiltersMatch } from './filter.js';
import { privatePeerEvent } from './public-peer-source.js';
import { expired } from './runtime-store.js';
import type { NostrFilter, NostrVerifiedEvent } from './types.js';
import type { FipsPubsubWireMessage } from './wire.js';

export const warmFeedId = /^wf1-ts-[0-9a-z]{1,12}$/;
export const publicFeedId = /^[cw]f1-ts-[0-9a-z]{1,12}$/;
type Sync = Extract<FipsPubsubWireMessage, { type: 'sync' }>;
type Event = NostrVerifiedEvent;
const hex = (id: Uint8Array) => Array.from(id, b => b.toString(16).padStart(2, '0')).join('');
const bytes = (id: string) => Uint8Array.from(id.match(/../g)!, s => parseInt(s, 16));

/** Owned by one explicit public carrier subscription. Never seeded from a general event cache. */
export class FeedWindow {
  private readonly records = new Map<string, Event>();
  constructor(readonly filter: NostrFilter, readonly limit: number) {}
  remember(event: Event): void {
    if (!this.accepts(event)) return;
    this.records.set(event.id, event);
    if (this.records.size > this.limit) this.records.delete(this.records.keys().next().value!);
  }
  accepts(event: Event): boolean {
    return !privatePeerEvent(event) && !expired(event) && subscriptionFiltersMatch([this.filter], event);
  }
  get(id: string) { return this.records.get(id); }
  snapshot(): Event[] { return [...this.records.values()].filter(event => this.accepts(event)); }
}

type State = {
  engine: Reconciliation; timer: ReturnType<typeof setTimeout>; busy: boolean;
  snapshot: Map<string, Event>; missing: Set<string>; owner: FeedWindow;
  finish?: (events: Event[], fallback?: boolean) => void;
};

/** Provider initiates so only its verified, missing events are offered through ordinary INV/WANT. */
export class FeedSync {
  private readonly offering = new Map<string, State>();
  private readonly receiving = new Map<string, State>();
  constructor(private readonly send: (peer: string, msg: Sync) => void, private readonly maxFrame: number,
    private readonly maxSessions: number) {}
  private start(map: Map<string, State>, key: string, owner: FeedWindow, events: Event[], timeout: () => void): State | undefined {
    const frameBytes = Math.min(8192, Math.floor((this.maxFrame - 96) / 2));
    if (frameBytes < 4096 || this.offering.size + this.receiving.size >= Math.min(16, this.maxSessions) ||
      map.has(key) || [...map.keys()].filter(k => k.startsWith(key.split(':')[0] + ':')).length >= 4) return;
    const records = events.map(event => ({ id: bytes(event.id), timestamp: BigInt(event.created_at) }));
    const engine = new Reconciliation(records, { since: 0n, until: (1n << 63n) - 1n }, { maxRecords: 256, maxFrameBytes: frameBytes, maxRounds: 16 });
    const state: State = { engine, busy: false, owner, snapshot: new Map(events.map(e => [e.id, e])), missing: new Set(),
      timer: setTimeout(() => { if (this.take(map, key)) timeout(); }, 2500) };
    map.set(key, state);
    return state;
  }
  private take(map: Map<string, State>, key: string): State | undefined {
    const state = map.get(key);
    if (state) { clearTimeout(state.timer); map.delete(key); }
    return state;
  }
  private frame(peer: string, id: string, action: Sync['action'], payload = '') {
    this.send(peer, { type: 'sync', action, subscriptionId: id, payload });
  }
  async offer(peer: string, id: string, owner: FeedWindow, inventory: Event[], finish: (events: Event[], fallback?: boolean) => void): Promise<void> {
    const key = peer + ':' + id;
    const fallback = () => { this.frame(peer, id, 'end'); finish(inventory, true); };
    const state = this.start(this.offering, key, owner, inventory, fallback);
    if (!state) { finish(inventory, true); return; }
    state.finish = finish;
    try {
      const opening = await state.engine.initiate({ fingerprintFirst: true });
      if (this.offering.get(key) === state) this.frame(peer, id, 'open', hex(opening));
    } catch { if (this.offering.get(key) === state) { this.take(this.offering, key); fallback(); } }
  }
  async receive(peer: string, message: Sync, local: FeedWindow | undefined, allowOpen: () => boolean): Promise<void> {
    const { subscriptionId: id, action, payload } = message, key = peer + ':' + id;
    const provider = action === 'reply' || action === 'decline';
    const map = provider ? this.offering : this.receiving;
    if (action === 'end') { this.take(map, key); return; }
    let state = map.get(key);
    if (action === 'open') {
      if (!local || state || !allowOpen()) return;
      state = this.start(map, key, local, local.snapshot(), () => this.frame(peer, id, 'decline'));
      if (!state) { this.frame(peer, id, 'decline'); return; }
    }
    if (!state) return;
    const current = state;
    const finish = (events: Event[], fallback = false) => {
      this.frame(peer, id, 'end');
      current.finish!(events, fallback);
    };
    if (action === 'decline') {
      this.take(map, key);
      finish([...current.snapshot.values()], true);
      return;
    }
    if (current.busy) return;
    current.busy = true;
    try {
      const frame = bytes(payload);
      if (!provider) {
        const reply = await current.engine.respond(frame);
        if (map.get(key) === current) this.frame(peer, id, 'reply', hex(reply));
      } else {
        const result = await current.engine.reconcile(frame);
        if (map.get(key) !== current) return;
        for (const id of result.have) {
          const hash = hex(id);
          if (current.snapshot.has(hash)) current.missing.add(hash);
        }
        if (result.next) this.frame(peer, id, 'step', hex(result.next));
        else {
          this.take(map, key);
          finish([...current.missing].map(id => current.snapshot.get(id)!));
        }
      }
    } catch {
      if (map.get(key) !== current) return;
      this.take(map, key);
      if (provider) finish([...current.snapshot.values()], true);
      else this.frame(peer, id, 'decline');
    } finally { current.busy = false; }
  }
  cancel(peer?: string, id?: string, owner?: FeedWindow, direction?: 'offer'): void {
    for (const map of direction ? [this.offering] : [this.offering, this.receiving]) for (const [key, state] of map) {
      if ((!peer || key.startsWith(peer + ':')) && (!id || key.endsWith(':' + id)) && (!owner || state.owner === owner)) this.take(map, key);
    }
  }
}
