import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { finalizeEvent, generateSecretKey } from 'nostr-tools/pure';
import { afterEach, describe, expect, it } from 'vitest';
import { FipsPubsubWireCodec, verifyNostrEvent, type FipsPubsubWireMessage, type NostrVerifiedEvent } from '../src/index.js';
import { FeedSync, FeedWindow } from '../src/fips-feed-sync.js';
import { validPublicFeedFilter } from '../src/public-peer-source.js';

type Sync = Extract<FipsPubsubWireMessage, { type: 'sync' }>;
type Output = { frames: string[]; finish: { events: string[]; fallback: boolean } | null };
const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString('hex');
const unhex = (text: string) => new Uint8Array(Buffer.from(text, 'hex'));
const codec = new FipsPubsubWireCodec();
const filter = { kinds: [7368, 30078] };
const key = generateSecretKey();
const roots = (count: number, label: string) => Array.from({ length: count }, (_, n) =>
  verifyNostrEvent(finalizeEvent({ kind: 30078, tags: [['d', `${label}-${n}`]], created_at: 1_700_000_000 + n, content: `${label} ${n}` }, key)));
const ids = (events: NostrVerifiedEvent[]) => events.map(event => event.id).sort();

class RustFeedSync {
  private readonly child = spawn('cargo', ['run', '--quiet', '--package', 'nostr-pubsub-fips', '--example', 'feed-sync-stdio'], {
    cwd: fileURLToPath(new URL('../../../../', import.meta.url)), stdio: 'pipe',
  });
  private readonly lines = createInterface({ input: this.child.stdout });
  private readonly iterator = this.lines[Symbol.asyncIterator]();
  private stderr = '';
  constructor() { this.child.stderr.on('data', (chunk: Buffer) => { this.stderr += chunk.toString(); }); }
  async command<T>(command: Record<string, unknown>): Promise<T> {
    this.child.stdin.write(`${JSON.stringify(command)}\n`);
    const line = await this.iterator.next();
    if (line.done) throw new Error(`Rust fixture exited early: ${this.stderr}`);
    const response = JSON.parse(line.value) as { ok: boolean; result: T; error?: string };
    if (!response.ok) throw new Error(response.error);
    return response.result;
  }
  async close() {
    this.child.stdin.end();
    if (this.child.exitCode === null) await once(this.child, 'exit');
    this.lines.close();
  }
}

const fixtures: RustFeedSync[] = [];
afterEach(async () => { await Promise.all(fixtures.splice(0).map(fixture => fixture.close())); });
async function rust() {
  const fixture = new RustFeedSync();
  fixtures.push(fixture);
  await fixture.command({ command: 'new' });
  return fixture;
}
/** Every frame crosses both codecs: TypeScript-encoded bytes into Rust and back. */
const toRust = (message: Sync) => hex(codec.encodeFrame(message));
const fromRust = (frame: string) => {
  const message = codec.decodeFrame(unhex(frame));
  if (message.type !== 'sync') throw new Error(`expected a sync frame, got ${message.type}`);
  return message;
};

describe('public feed sync between Rust and TypeScript peers', () => {
  it('a TypeScript provider announces only what a Rust subscriber lacks', async () => {
    const native = await rust();
    const inventory = roots(40, 'web');
    const held = [...inventory.slice(0, 25), ...inventory.slice(30)];
    const sent: Sync[] = [];
    const finished: Array<{ events: NostrVerifiedEvent[]; fallback?: boolean }> = [];
    const sync = new FeedSync((_peer, message) => { sent.push(message); }, 65_525, 4);
    await sync.offer('rust', 'wf1-ts-1', new FeedWindow(filter, 256), inventory, (events, fallback) => { finished.push({ events, fallback }); });
    const actions: string[] = [];
    while (!finished.length) {
      const message = sent.shift();
      if (!message) throw new Error('TypeScript provider stalled');
      actions.push(message.action);
      const output = await native.command<Output>({ command: 'receive', peer: 'web', frame: toRust(message), window: held });
      for (const frame of output.frames) {
        const reply = fromRust(frame);
        actions.push(reply.action);
        await sync.receive('rust', reply, undefined, () => false);
      }
    }
    actions.push(...sent.map(message => message.action));
    expect(actions[0]).toBe('open');
    expect(actions.at(-1)).toBe('end');
    expect(finished).toHaveLength(1);
    expect(finished[0].fallback).toBeFalsy();
    expect(ids(finished[0].events)).toEqual(ids(inventory.slice(25, 30)));
  }, 120_000);

  it('a Rust provider announces only what a TypeScript subscriber lacks, and ends an in-sync window in one round', async () => {
    const native = await rust();
    const inventory = roots(40, 'native');
    for (const [held, missing, id] of [
      [[...inventory.slice(0, 10), ...inventory.slice(12)], inventory.slice(10, 12), 'wf1-ts-2'],
      [inventory, [], 'wf1-ts-3'],
    ] as const) {
      const window = new FeedWindow(filter, 256);
      for (const event of held) window.remember(event);
      const sent: Sync[] = [];
      const sync = new FeedSync((_peer, message) => { sent.push(message); }, 65_525, 4);
      let output = await native.command<Output>({ command: 'offer', peer: 'web', id, events: inventory });
      let opened = false, rounds = 0;
      const actions: string[] = [];
      while (!output.finish) {
        expect(output.frames).toHaveLength(1);
        const frame = fromRust(output.frames[0]);
        actions.push(frame.action);
        await sync.receive('rust', frame, window, () => !opened && (opened = true));
        const reply = sent.shift();
        if (!reply) throw new Error('TypeScript subscriber did not reply');
        actions.push(reply.action);
        output = await native.command<Output>({ command: 'receive', peer: 'web', frame: toRust(reply), window: null });
        rounds++;
      }
      const end = fromRust(output.frames.at(-1)!);
      actions.push(end.action);
      await sync.receive('rust', end, window, () => false);
      expect(actions[0]).toBe('open');
      expect(end.action).toBe('end');
      expect(output.finish.fallback).toBe(false);
      expect(output.finish.events.sort()).toEqual(ids([...missing]));
      if (!missing.length) expect(rounds).toBe(1);
    }
  }, 120_000);

  it('a Rust subscriber declines an unsolicited session and a TypeScript provider falls back once', async () => {
    const native = await rust();
    const inventory = roots(3, 'fallback');
    const sent: Sync[] = [];
    const finished: Array<{ events: NostrVerifiedEvent[]; fallback?: boolean }> = [];
    const sync = new FeedSync((_peer, message) => { sent.push(message); }, 65_525, 4);
    await sync.offer('rust', 'wf1-ts-4', new FeedWindow(filter, 256), inventory, (events, fallback) => { finished.push({ events, fallback }); });
    const unsolicited = await native.command<Output>({ command: 'receive', peer: 'web', frame: toRust(sent.shift()!), window: null });
    expect(unsolicited.frames).toEqual([]);
    sync.cancel('rust', 'wf1-ts-4');
    const tiny = new RustFeedSync();
    fixtures.push(tiny);
    await tiny.command({ command: 'new', maxSessions: 0 });
    await sync.offer('rust', 'wf1-ts-5', new FeedWindow(filter, 256), inventory, (events, fallback) => { finished.push({ events, fallback }); });
    const declined = await tiny.command<Output>({ command: 'receive', peer: 'web', frame: toRust(sent.shift()!), window: [] });
    const decline = fromRust(declined.frames[0]);
    expect(decline.action).toBe('decline');
    await sync.receive('rust', decline, undefined, () => false);
    expect(finished).toEqual([{ events: inventory, fallback: true }]);
    expect(sent.map(message => message.action)).toEqual(['end']);
  }, 120_000);

  it('a Rust feed request has the TypeScript feed id and filter', async () => {
    const native = await rust();
    for (const warm of [true, false]) {
      const { frame, valid } = await native.command<{ frame: string; valid: boolean }>({ command: 'request', key: 'ts-1z', warm, filter });
      const request = codec.decodeFrame(unhex(frame));
      expect(valid).toBe(true);
      expect(request).toEqual({ type: 'req', subscriptionId: `${warm ? 'w' : 'c'}f1-ts-1z`, filters: [filter] });
      if (request.type === 'req') expect(validPublicFeedFilter(request.filters[0])).toBe(true);
    }
    const limited = await native.command<{ valid: boolean }>({ command: 'request', key: 'ts-2', warm: false, filter: { ...filter, limit: 1 } });
    expect(limited.valid).toBe(validPublicFeedFilter({ ...filter, limit: 1 }));
  }, 120_000);
});
