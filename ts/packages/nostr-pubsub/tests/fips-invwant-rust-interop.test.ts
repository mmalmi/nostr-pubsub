import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';
import { createInterface, type Interface } from 'node:readline';
import { afterEach, describe, expect, it } from 'vitest';
import { Stack, State } from '@fips/tcp';
import type { Event } from 'nostr-tools/core';
import vectors from '../../../../crates/nostr-pubsub/tests/data/interop-vectors.json';
import {
  FipsInvWantStream,
  verifyNostrEvent,
  type FipsInvWantStreamAction,
} from '../src/index.js';

interface FixtureRecord {
  peer_id: string;
  record: string;
}

interface FixtureResponse {
  ok: boolean;
  records: FixtureRecord[];
  deliveries: string[];
  result: unknown;
  outbound: Array<{ peer: string; bytes: string }>;
  error?: string;
}

class RustInvWantFixture {
  private readonly child: ChildProcessWithoutNullStreams;
  private readonly lines: Interface;
  private readonly iterator: AsyncIterableIterator<string>;
  private stderr = '';

  constructor() {
    this.child = spawn('cargo', [
      'run',
      '--quiet',
      '--package',
      'nostr-pubsub-fips',
      '--example',
      'inv-want-stdio',
    ], {
      cwd: fileURLToPath(new URL('../../../../', import.meta.url)),
      stdio: 'pipe',
    });
    this.child.stderr.setEncoding('utf8');
    this.child.stderr.on('data', (chunk: string) => { this.stderr += chunk; });
    this.lines = createInterface({ input: this.child.stdout });
    this.iterator = this.lines[Symbol.asyncIterator]();
  }

  async command(command: Record<string, unknown>): Promise<FixtureResponse> {
    this.child.stdin.write(`${JSON.stringify(command)}\n`);
    const line = await this.iterator.next();
    if (line.done) throw new Error(`Rust fixture exited early: ${this.stderr}`);
    const response = JSON.parse(line.value) as FixtureResponse;
    if (!response.ok) throw new Error(response.error ?? 'Rust fixture command failed');
    return response;
  }

  async close(): Promise<void> {
    this.child.stdin.end();
    if (this.child.exitCode === null) await once(this.child, 'exit');
    this.lines.close();
    if (this.child.exitCode !== 0) throw new Error(`Rust fixture failed: ${this.stderr}`);
  }
}

const fixtures: RustInvWantFixture[] = [];
afterEach(async () => {
  await Promise.all(fixtures.splice(0).map((fixture) => fixture.close()));
});

describe('live Rust/TypeScript reliable Inv/WANT records', () => {
  it('exchanges canonical records in both directions through a Rust process', async () => {
    const rust = new RustInvWantFixture();
    fixtures.push(rust);
    const typescript = new FipsInvWantStream();
    const fromTypescript = verifyNostrEvent(vectors.events.fipsAdvert as Event);
    const inventory = onlyRecord(typescript.publish(fromTypescript, ['rust'], 1), 'rust');

    const partial = await rust.command(receive(inventory.subarray(0, 2), 2));
    expect(partial.records).toEqual([]);
    const wantResponse = await rust.command(receive(inventory.subarray(2), 3));
    const want = onlyFixtureRecord(wantResponse, 'typescript');
    const frame = onlyRecord(
      await typescript.receiveBytes('rust', want, ['rust'], 4),
      'rust',
    );
    const delivered = await rust.command(receive(frame, 5));
    expect(delivered.deliveries).toEqual([fromTypescript.id]);

    const fromRust = verifyNostrEvent(vectors.events.hashtreeRoot as Event);
    const rustInventory = onlyFixtureRecord(await rust.command({
      op: 'publish',
      event: fromRust,
      connected_peers: ['typescript'],
      now_ms: 6,
    }), 'typescript');
    const typescriptWant = onlyRecord(
      await typescript.receiveBytes('rust', rustInventory, ['rust'], 7),
      'rust',
    );
    const rustFrame = onlyFixtureRecord(
      await rust.command(receive(typescriptWant, 8)),
      'typescript',
    );
    const typescriptDelivery = await typescript.receiveBytes(
      'rust',
      rustFrame,
      ['rust'],
      9,
    );
    expect(deliveredIds(typescriptDelivery)).toEqual([fromRust.id]);
  }, 60_000);

  it('carries those records through the live Rust/TypeScript TCP wire', async () => {
    const rust = new RustInvWantFixture();
    fixtures.push(rust);
    const pair = new TcpCrossPair(rust);
    const port = 39_121;
    await pair.rustCommand({ op: 'tcp_listen', port });
    const typescriptConnection = pair.typescriptTcp.connect('rust', port, pair.nowMs);
    await pair.settle();
    const accepted = await pair.rustCommand({ op: 'tcp_accept', port });
    const rustConnection = Number(accepted.result);
    expect(pair.typescriptTcp.state(typescriptConnection)).toBe(State.Established);
    expect((await pair.rustCommand({ op: 'tcp_state', id: rustConnection })).result)
      .toBe('established');

    const typescriptStream = new FipsInvWantStream();
    const fromTypescript = verifyNostrEvent(vectors.events.socialNote as Event);
    const inventory = onlyRecord(
      typescriptStream.publish(fromTypescript, ['rust'], 1),
      'rust',
    );
    expect(pair.typescriptTcp.write(typescriptConnection, inventory, pair.nowMs))
      .toBe(inventory.byteLength);
    await pair.settle();
    const rustWant = onlyFixtureRecord(
      await pair.rustCommand(receive(await pair.readRust(rustConnection), 2)),
      'typescript',
    );
    await pair.writeRust(rustConnection, rustWant);
    const typescriptWant = pair.typescriptTcp.read(typescriptConnection, 64 * 1024, pair.nowMs);
    const frame = onlyRecord(
      await typescriptStream.receiveBytes('rust', typescriptWant, ['rust'], 3),
      'rust',
    );
    expect(pair.typescriptTcp.write(typescriptConnection, frame, pair.nowMs))
      .toBe(frame.byteLength);
    await pair.settle();
    const rustDelivery = await pair.rustCommand(receive(
      await pair.readRust(rustConnection),
      4,
    ));
    expect(rustDelivery.deliveries).toEqual([fromTypescript.id]);

    const fromRust = verifyNostrEvent(vectors.events.relayNote as Event);
    const rustInventory = onlyFixtureRecord(await pair.rustCommand({
      op: 'publish',
      event: fromRust,
      connected_peers: ['typescript'],
      now_ms: 5,
    }), 'typescript');
    await pair.writeRust(rustConnection, rustInventory);
    const typescriptInventory = pair.typescriptTcp.read(
      typescriptConnection,
      64 * 1024,
      pair.nowMs,
    );
    const want = onlyRecord(
      await typescriptStream.receiveBytes('rust', typescriptInventory, ['rust'], 6),
      'rust',
    );
    expect(pair.typescriptTcp.write(typescriptConnection, want, pair.nowMs))
      .toBe(want.byteLength);
    await pair.settle();
    const rustFrame = onlyFixtureRecord(
      await pair.rustCommand(receive(await pair.readRust(rustConnection), 7)),
      'typescript',
    );
    await pair.writeRust(rustConnection, rustFrame);
    const typescriptFrame = pair.typescriptTcp.read(
      typescriptConnection,
      64 * 1024,
      pair.nowMs,
    );
    expect(deliveredIds(await typescriptStream.receiveBytes(
      'rust',
      typescriptFrame,
      ['rust'],
      8,
    ))).toEqual([fromRust.id]);

    pair.typescriptTcp.close(typescriptConnection, pair.nowMs);
    await pair.settle();
    expect((await pair.rustCommand({ op: 'tcp_state', id: rustConnection })).result)
      .toBe('close-wait');
    await pair.rustCommand({ op: 'tcp_close', id: rustConnection, now_ms: pair.nowMs });
    await pair.settle();
    pair.nowMs += 60_000;
    await pair.settle();
    expect(pair.typescriptTcp.state(typescriptConnection)).toBeUndefined();
    expect((await pair.rustCommand({ op: 'tcp_state', id: rustConnection })).result).toBeNull();
  }, 60_000);
});

class TcpCrossPair {
  readonly typescriptTcp = new Stack({}, 0x1234_5678_9abc_def0n);
  nowMs = 0;
  private readonly rustOutbound: Uint8Array[] = [];

  constructor(private readonly rust: RustInvWantFixture) {}

  async rustCommand(command: Record<string, unknown>): Promise<FixtureResponse> {
    const response = await this.rust.command(command);
    this.rustOutbound.push(...response.outbound.map(
      (outbound) => Uint8Array.from(Buffer.from(outbound.bytes, 'hex')),
    ));
    return response;
  }

  async settle(): Promise<void> {
    for (let attempt = 0; attempt < 256; attempt += 1) {
      if (await this.step() === 0) return;
    }
    throw new Error('Rust/TypeScript TCP pair did not settle');
  }

  async readRust(id: number): Promise<Uint8Array> {
    const response = await this.rustCommand({
      op: 'tcp_read',
      id,
      max: 64 * 1024,
      now_ms: this.nowMs,
    });
    return Uint8Array.from(Buffer.from(String(response.result), 'hex'));
  }

  async writeRust(id: number, bytes: Uint8Array): Promise<void> {
    const response = await this.rustCommand({
      op: 'tcp_write',
      id,
      bytes: Buffer.from(bytes).toString('hex'),
      now_ms: this.nowMs,
    });
    expect(response.result).toBe(bytes.byteLength);
    await this.settle();
  }

  private async step(): Promise<number> {
    this.typescriptTcp.poll(this.nowMs);
    await this.rustCommand({ op: 'tcp_poll', now_ms: this.nowMs });
    const fromTypescript = this.typescriptTcp.drainOutbound().map((item) => item.bytes);
    const fromRust = this.rustOutbound.splice(0);
    for (const bytes of fromTypescript) {
      await this.rustCommand({
        op: 'tcp_input',
        peer: 'typescript',
        bytes: Buffer.from(bytes).toString('hex'),
        now_ms: this.nowMs,
      });
    }
    for (const bytes of fromRust) this.typescriptTcp.input('rust', bytes, this.nowMs);
    return fromTypescript.length + fromRust.length;
  }
}

function receive(bytes: Uint8Array, nowMs: number): Record<string, unknown> {
  return {
    op: 'receive',
    source_peer: 'typescript',
    bytes: Buffer.from(bytes).toString('hex'),
    connected_peers: ['typescript'],
    now_ms: nowMs,
  };
}

function onlyFixtureRecord(response: FixtureResponse, expectedPeer: string): Uint8Array {
  expect(response.records).toHaveLength(1);
  expect(response.records[0]?.peer_id).toBe(expectedPeer);
  return Uint8Array.from(Buffer.from(response.records[0]!.record, 'hex'));
}

function onlyRecord(
  actions: readonly FipsInvWantStreamAction[],
  expectedPeer: string,
): Uint8Array {
  const records = actions.filter(
    (action): action is Extract<FipsInvWantStreamAction, { type: 'send' }> =>
      action.type === 'send',
  );
  expect(records).toHaveLength(1);
  expect(records[0]?.peerId).toBe(expectedPeer);
  return records[0]!.record;
}

function deliveredIds(actions: readonly FipsInvWantStreamAction[]): string[] {
  return actions.flatMap((action) => action.type === 'deliver' ? [action.event.event.id] : []);
}
