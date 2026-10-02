// Negentropy v1 range reconciliation, adapted from Doug Hoyte's MIT-licensed
// reference implementation. See LICENSE. BigInt preserves the full u64 clock.
import { compare, compareIds, INFINITY, key, Reader, Writer, type Bound } from './encoding.js';
import { Storage } from './storage.js';
import { validateFrame } from './validation.js';

/** IDs must be cryptographic hashes, e.g. Nostr event IDs, not padded counters. */
export interface ReconciliationRecord { timestamp: bigint; id: Uint8Array }
export interface ReconciliationFilter { since: bigint; until: bigint }
export interface ReconciliationStep { next?: Uint8Array; have: Uint8Array[]; need: Uint8Array[] }
export interface ReconciliationLimits { maxRecords?: number; maxFrameBytes?: number; maxRounds?: number }
const infinity: Bound = { timestamp: INFINITY, id: new Uint8Array() };

/** Immutable bounded snapshot. Callers own authentication, storage and transfer.
 * Completion means the chosen window was compared, not that records were saved.
 * Browser/worker native TypeScript; no WebAssembly or Node APIs. */
export class Reconciliation {
  private storage: Storage;
  private frameBytes: number;
  private maxRounds: number;
  private rounds = 0;
  private initiator = false;
  private finished = false;
  private busy = false;

  constructor(records: ReconciliationRecord[], filter: ReconciliationFilter, limits: ReconciliationLimits = {}) {
    const maxRecords = limits.maxRecords ?? 100_000;
    this.frameBytes = limits.maxFrameBytes ?? 16_384;
    this.maxRounds = limits.maxRounds ?? 256;
    if (filter.since < 0n || filter.until >= INFINITY || filter.since > filter.until
      || !Number.isInteger(maxRecords) || maxRecords < 1 || maxRecords > 1_000_000
      || !Number.isInteger(this.frameBytes) || this.frameBytes < 4096 || this.frameBytes > 65_536
      || !Number.isInteger(this.maxRounds) || this.maxRounds < 1 || records.length > 1_000_000) {
      throw new Error('invalid reconciliation filter or limits');
    }
    for (const record of records) {
      if (record.id.length !== 32 || record.timestamp < 0n || record.timestamp >= INFINITY) throw new Error('invalid record');
    }
    this.storage = new Storage(records, filter.since, filter.until, maxRecords);
  }

  async initiate(): Promise<Uint8Array> {
    if (this.initiator) throw new Error('already initiated');
    this.enter();
    try {
      this.initiator = true;
      const out = new Writer(); out.append([0x61]);
      await this.split(0, this.storage.items.length, infinity, out);
      return out.finish();
    } finally { this.busy = false; }
  }
  async respond(frame: Uint8Array): Promise<Uint8Array> {
    if (this.initiator) throw new Error('initiator cannot respond');
    return (await this.process(frame)).next!;
  }
  async reconcile(frame: Uint8Array): Promise<ReconciliationStep> {
    if (!this.initiator) throw new Error('session is not initiator');
    return this.process(frame);
  }
  private enter(): void {
    if (this.busy || this.finished || this.rounds >= this.maxRounds) throw new Error('session busy, finished or round limit exceeded');
    this.rounds++; this.busy = true;
  }
  private async process(frame: Uint8Array): Promise<ReconciliationStep> {
    this.enter();
    try {
      if (frame.length > this.frameBytes) throw new Error('frame exceeds byte limit');
      // Buffer.slice() aliases, so use the typed-array copy constructor.
      const owned = new Uint8Array(frame);
      validateFrame(owned);
      const input = new Reader(owned);
      if (input.take(1)[0] !== 0x61) throw new Error('unsupported Negentropy version');
      const output = new Writer(); output.append([0x61]);
      const have: Uint8Array[] = [], need: Uint8Array[] = [];
      let previous: Bound = { timestamp: 0n, id: new Uint8Array() };
      let previousIndex = 0, skip = false;
      while (input.remaining) {
        const current = input.bound();
        if (compare(current, previous) < 0) throw new Error('unordered ranges');
        const mode = input.varint();
        const lower = previousIndex;
        let upper = this.storage.lowerBound(previousIndex, current);
        let truncated = false;
        const out = new Writer(); out.timestamp = output.timestamp;
        const flushSkip = () => { if (skip) { skip = false; out.bound(previous); out.varint(0n); } };
        if (mode === 0n) skip = true;
        else if (mode === 1n) {
          const theirs = input.take(16);
          const ours = await this.storage.fingerprint(lower, upper);
          if (compareIds(theirs, ours)) { flushSkip(); await this.split(lower, upper, current, out); }
          else skip = true;
        } else if (mode === 2n) {
          const count = input.varint();
          if (count > BigInt(Math.floor(input.remaining / 32))) throw new Error('truncated ID list');
          const theirs = new Map<string, Uint8Array>();
          for (let i = 0n; i < count; i++) { const id = input.take(32); if (this.initiator) theirs.set(key(id), id); }
          if (this.initiator) {
            skip = true;
            for (let i = lower; i < upper; i++) {
              const id = this.storage.items[i].id;
              if (!theirs.delete(key(id))) have.push(id.slice());
            }
            need.push(...[...theirs.values()].map(id => id.slice()));
          } else {
            flushSkip();
            const ids: Uint8Array[] = [];
            let endBound = current;
            for (let i = lower; i < upper; i++) {
              if (output.length + ids.length * 32 > this.frameBytes - 200) { endBound = this.storage.items[i]; upper = i; truncated = true; break; }
              ids.push(this.storage.items[i].id);
            }
            out.bound(endBound); out.varint(2n); out.varint(BigInt(ids.length));
            for (const id of ids) out.append(id);
            output.append(out.bytes); output.timestamp = out.timestamp;
            out.bytes = [];
          }
        } else throw new Error('invalid reconciliation mode');
        if (truncated || output.length + out.length > this.frameBytes - 200) {
          if (!truncated) { output.bound(out.length ? previous : current); output.varint(0n); }
          output.bound(infinity); output.varint(1n);
          output.append(await this.storage.fingerprint(out.length ? lower : upper, this.storage.items.length));
          break;
        }
        output.append(out.bytes); output.timestamp = out.timestamp;
        previous = current; previousIndex = upper;
      }
      const next = output.length === 1 && this.initiator ? undefined : output.finish();
      this.finished = next === undefined;
      return { next, have, need };
    } finally { this.busy = false; }
  }

  private async split(lower: number, upper: number, bound: Bound, out: Writer): Promise<void> {
    const count = upper - lower;
    if (count < 32) {
      out.bound(bound); out.varint(2n); out.varint(BigInt(count));
      for (let i = lower; i < upper; i++) out.append(this.storage.items[i].id);
      return;
    }
    let cursor = lower;
    for (let i = 0; i < 16; i++) {
      const size = Math.floor(count / 16) + (i < count % 16 ? 1 : 0);
      const fingerprint = await this.storage.fingerprint(cursor, cursor + size);
      cursor += size;
      let next = bound;
      if (cursor !== upper) {
        const before = this.storage.items[cursor - 1], after = this.storage.items[cursor];
        let prefix = 0;
        if (before.timestamp === after.timestamp) { while (before.id[prefix] === after.id[prefix]) prefix++; prefix++; }
        next = { timestamp: after.timestamp, id: after.id.subarray(0, prefix) };
      }
      out.bound(next); out.varint(1n); out.append(fingerprint);
    }
  }
}
