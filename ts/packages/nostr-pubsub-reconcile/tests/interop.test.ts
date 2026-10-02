import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { describe, it, expect } from 'vitest';
import { Reconciliation, type ReconciliationRecord } from '../src/index.js';

const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString('hex');
const bytes = (hex: string) => new Uint8Array(Buffer.from(hex, 'hex'));
function record(n: number): ReconciliationRecord {
  const id = new Uint8Array(createHash('sha256').update(String(n)).digest());
  return { timestamp: 5_000_000_000n + BigInt(Math.floor(n / 3)), id };
}

describe('native and TypeScript Negentropy peers', () => {
  for (const browserInitiates of [true, false]) {
    it(`exchanges real bounded frames with ${browserInitiates ? 'TypeScript' : 'Rust'} initiating`, async () => {
      const rust = spawn('cargo', ['run', '--quiet', '-p', 'nostr-pubsub-reconcile', '--example', 'reconcile-stdio'], {
        cwd: fileURLToPath(new URL('../../../../', import.meta.url)), stdio: 'pipe',
      });
      let errors = ''; rust.stderr.on('data', v => { errors += v; });
      const reader = createInterface({ input: rust.stdout });
      const lines = reader[Symbol.asyncIterator]();
      async function command(value: unknown) {
        rust.stdin.write(JSON.stringify(value) + '\n');
        const line = await lines.next();
        if (line.done) throw new Error(errors);
        const output = JSON.parse(line.value);
        if (!output.ok) throw new Error(output.error);
        return output.result;
      }
      try {
        const all = Array.from({ length: 10_000 }, (_, n) => record(n));
        const a = all.filter((_, n) => n !== 7 && n % 7 !== 0);
        const b = all.filter((_, n) => n !== 11 && n % 11 !== 0);
        const filter = { since: record(3).timestamp, until: record(9990).timestamp };
        const typescript = new Reconciliation(a, filter, { maxFrameBytes: 4096 });
        await command({ command: 'new', records: b.map(r => ({ timestamp: String(r.timestamp), id: hex(r.id) })), since: String(filter.since), until: String(filter.until), frameBytes: 4096 });
        let frame = browserInitiates ? await typescript.initiate() : bytes((await command({ command: 'initiate' })).next);
        const have: string[] = [], need: string[] = [];
        for (let round = 0; ; round++) {
          expect(round).toBeLessThan(256); expect(frame.length).toBeLessThanOrEqual(4096);
          if (browserInitiates) {
            const response = await command({ command: 'respond', frame: hex(frame) });
            const step = await typescript.reconcile(bytes(response.next));
            have.push(...step.have.map(hex)); need.push(...step.need.map(hex));
            if (!step.next) break;
            frame = step.next;
          } else {
            const response = await typescript.respond(frame);
            const step = await command({ command: 'reconcile', frame: hex(response) });
            have.push(...step.have); need.push(...step.need);
            if (!step.next) break;
            frame = bytes(step.next);
          }
        }
        const eligible = (records: ReconciliationRecord[]) => new Set(records.filter(r => r.timestamp >= filter.since && r.timestamp <= filter.until).map(r => hex(r.id)));
        const local = eligible(browserInitiates ? a : b), remote = eligible(browserInitiates ? b : a);
        expect([...new Set(have)].sort()).toEqual([...local].filter(id => !remote.has(id)).sort());
        expect([...new Set(need)].sort()).toEqual([...remote].filter(id => !local.has(id)).sort());
        expect(have.length).toBe(new Set(have).size); expect(need.length).toBe(new Set(need).size);
      } finally {
        rust.stdin.end();
        if (rust.exitCode === null) await once(rust, 'exit');
        reader.close();
      }
    }, 60_000);
  }
});

it('rejects malformed frames, excessive history and conflicting IDs', async () => {
  const filter = { since: 0n, until: 10_000_000_000n };
  expect(() => new Reconciliation([record(1), record(2)], filter, { maxRecords: 1 })).toThrow();
  expect(() => new Reconciliation([record(1), { ...record(1), timestamp: 1n }], filter)).toThrow();
  const peer = new Reconciliation([], filter);
  for (const frame of [new Uint8Array(), new Uint8Array([0x61, 1]), new Uint8Array([0x61, 0, 33]), new Uint8Array(20_000)]) {
    await expect(peer.respond(frame)).rejects.toThrow();
  }
});

it('owns Buffer record IDs and frames across asynchronous fingerprinting', async () => {
  const original = Array.from({length: 1000}, (_, n) => record(n));
  const mutable = [original[0]].map(r => ({...r, id: Buffer.from(r.id)}));
  const filter = {since: 0n, until: 10_000_000_000n};
  const snapshot = new Reconciliation(mutable, filter, {maxFrameBytes: 4096});
  for (const r of mutable) r.id.fill(0);
  const expected = new Reconciliation([original[0]], filter, {maxFrameBytes: 4096});
  expect(await snapshot.initiate()).toEqual(await expected.initiate());

  const query = await new Reconciliation(original.filter((_, n) => n % 2), filter, {maxFrameBytes: 4096}).initiate();
  const fromBuffer = new Reconciliation(original, filter, {maxFrameBytes: 4096});
  const queryBuffer = Buffer.from(query);
  const response = fromBuffer.respond(queryBuffer);
  queryBuffer.fill(0xff);
  const reference = await new Reconciliation(original, filter, {maxFrameBytes: 4096}).respond(query);
  expect(await response).toEqual(reference);
});

it('rejects a malformed tail even when an earlier range fills the response frame', async () => {
  const records = Array.from({length:1000}, (_, n) => record(n));
  const peer = new Reconciliation(records, {since:0n,until:10_000_000_000n}, {maxFrameBytes:4096});
  await expect(peer.respond(new Uint8Array([0x61,0,0,2,0,0xff]))).rejects.toThrow('truncated');
});

it('reconciles distinct Buffer subarrays with nonzero offsets correctly', async () => {
  const slab = Buffer.alloc(64 * 32);
  for (let n = 0; n < 64; n++) Buffer.from(record(n).id).copy(slab, n * 32);
  const records = Array.from({length:64}, (_, n) => ({
    timestamp:BigInt(n % 32), id:slab.subarray(n * 32, (n + 1) * 32),
  }));
  const filter = {since:0n,until:31n};
  const a = new Reconciliation(records.slice(0,32), filter);
  const b = new Reconciliation(records.slice(32), filter);
  let query = await a.initiate();
  const have: string[] = [], need: string[] = [];
  for (;;) {
    const step = await a.reconcile(await b.respond(query));
    have.push(...step.have.map(hex)); need.push(...step.need.map(hex));
    if (!step.next) break;
    query = step.next;
  }
  expect(have.sort()).toEqual(records.slice(0,32).map(r=>hex(r.id)).sort());
  expect(need.sort()).toEqual(records.slice(32).map(r=>hex(r.id)).sort());
});
