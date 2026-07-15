import { describe, expect, it } from 'vitest';
import type {
  FipsDatagramEndpoint,
  FipsServiceContext,
} from '@fips/tcp';
import { finalizeEvent } from 'nostr-tools/pure';
import {
  FipsInvWantStream,
  FipsInvWantTcpDriver,
  fipsInvWantTcpCapabilityName,
  fipsInvWantTcpPeerOrderKey,
  verifyNostrEvent,
  type FipsInvWantTcpDriveReport,
  type FipsInvWantTcpDriverOptions,
  type NostrVerifiedEvent,
} from '../src/index.js';

type ServiceHandler = (context: FipsServiceContext) => Promise<void> | void;

class MemoryFipsEndpoint implements FipsDatagramEndpoint {
  private readonly services = new Map<number, ServiceHandler>();
  private remote?: MemoryFipsEndpoint;

  constructor(readonly identity: string) {}

  connect(remote: MemoryFipsEndpoint): void {
    this.remote = remote;
  }

  registerService(port: number, handler: ServiceHandler): () => void {
    if (this.services.has(port)) throw new Error(`service ${port} is already registered`);
    this.services.set(port, handler);
    return () => this.services.delete(port);
  }

  async sendDatagram(args: {
    dst: string;
    srcPort?: number;
    dstPort: number;
    payload: Uint8Array;
  }): Promise<void> {
    const remote = this.remote;
    if (remote === undefined || remote.identity !== args.dst) throw new Error('unknown peer');
    remote.deliver({
      src: this.identity,
      srcPort: args.srcPort ?? 0,
      dstPort: args.dstPort,
      payload: args.payload.slice(),
    });
  }

  inject(context: FipsServiceContext): void {
    this.deliver(context);
  }

  hasService(port: number): boolean {
    return this.services.has(port);
  }

  private deliver(context: FipsServiceContext): void {
    const handler = this.services.get(context.dstPort);
    if (handler === undefined) throw new Error(`service ${context.dstPort} is not registered`);
    queueMicrotask(() => void handler(context));
  }
}

const servicePort = 39_121;

describe('reliable Inv/WANT TCP/FIPS driver', () => {
  it('converges simultaneous authenticated connects and replays across reset', async () => {
    const pair = driverPair();
    try {
      expect(fipsInvWantTcpCapabilityName(options())).toBe('test.nostr.pubsub.stream/1');
      await Promise.all([
        pair.alice.connectPeer('bob', 0),
        pair.bob.connectPeer('alice', 0),
      ]);
      await pump(pair, 1, (outcome) =>
        outcome.aliceConnected === 1 && outcome.bobConnected === 1);

      const first = signedEvent('x'.repeat(96 * 1024), 7);
      pair.alice.publish(first, 10);
      const delivered = await pump(pair, 11, (outcome) => outcome.bobIds.has(first.id));
      expect(delivered.bobIds).toContain(first.id);
      expect(delivered.bobStreamBytes).toBeGreaterThan(0xffff);

      await pair.alice.abortPeer('bob');
      await pump(pair, 100, (outcome) =>
        outcome.aliceConnected === 0 && outcome.bobConnected === 0);
      const second = signedEvent('offline replay', 8);
      expect(pair.alice.publish(second, 200)).toEqual({ peers: 0, records: 0, bytes: 0 });
      await pair.alice.connectPeer('bob', 201);
      const replayed = await pump(pair, 202, (outcome) => outcome.bobIds.has(second.id));
      expect(replayed.bobIds).toContain(second.id);
    } finally {
      await pair.dispose();
    }
  }, 20_000);

  it('rejects queue overflow transactionally and isolates malformed TCP input', async () => {
    const pair = driverPair({
      maxQueuedRecordsPerPeer: 1,
      maxQueuedBytesPerPeer: 1024,
    });
    try {
      await pair.alice.connectPeer('bob', 0);
      await pump(pair, 1, (outcome) => outcome.aliceConnected === 1);
      pair.alice.publish(signedEvent('fits', 9), 10);
      expect(() => pair.alice.publish(signedEvent('must wait', 10), 11)).toThrow(/queue/);
      expect(pair.alice.queueSnapshot()).toMatchObject({ peers: 1, records: 1 });
      expect(pair.alice.queueSnapshot().bytes).toBeLessThanOrEqual(1024);

      pair.bobEndpoint.inject({
        src: 'alice',
        srcPort: servicePort,
        dstPort: servicePort,
        payload: Uint8Array.of(1, 2, 3),
      });
      await tick();
      const report = await pair.bob.receive(20);
      expect(report.rejectedTcpSegments).toBe(1);
      expect(report.fipsDatagrams).toBe(1);
    } finally {
      await pair.dispose();
    }
  });

  it('uses Rust npub ordering to converge compressed-key simultaneous connects', async () => {
    const left = `03${'00'.repeat(32)}`;
    const right = `02${`01${'11'.repeat(31)}`}`;
    expect(left < right).toBe(false);
    expect(fipsInvWantTcpPeerOrderKey(left) < fipsInvWantTcpPeerOrderKey(right)).toBe(true);
    expect(fipsInvWantTcpPeerOrderKey(fipsInvWantTcpPeerOrderKey(left)))
      .toBe(fipsInvWantTcpPeerOrderKey(left));

    const pair = driverPair({}, { alice: left, bob: right });
    try {
      await Promise.all([
        pair.alice.connectPeer(right, 0),
        pair.bob.connectPeer(left, 0),
      ]);
      await pump(pair, 1, (outcome) =>
        outcome.aliceConnected === 1 && outcome.bobConnected === 1);
    } finally {
      await pair.dispose();
    }
  });

  it('bounds options and unregisters its listener on disposal', async () => {
    const endpoint = new MemoryFipsEndpoint('local');
    expect(() => FipsInvWantTcpDriver.bind(
      endpoint,
      'local',
      new FipsInvWantStream(),
      options({ maxPeers: 0 }),
    )).toThrow(/max peers/);

    const driver = FipsInvWantTcpDriver.bind(
      endpoint,
      'local',
      new FipsInvWantStream(),
      options(),
    );
    expect(endpoint.hasService(servicePort)).toBe(true);
    await driver.dispose();
    expect(endpoint.hasService(servicePort)).toBe(false);
    await driver.dispose();
  });

  it('preserves endpoint connect errors without leaking bounded peer capacity', async () => {
    const endpoint = new MemoryFipsEndpoint('local');
    const driver = FipsInvWantTcpDriver.bind(
      endpoint,
      'local',
      new FipsInvWantStream(),
      options({ maxPeers: 1 }),
    );
    try {
      for (let attempt = 0; attempt < 3; attempt += 1) {
        await expect(driver.connectPeer('offline', attempt)).rejects.toThrow(/unknown peer/);
      }
      expect(driver.connectedPeerCount()).toBe(0);
    } finally {
      await driver.dispose();
    }
  });
});

interface DriverPair {
  alice: FipsInvWantTcpDriver;
  bob: FipsInvWantTcpDriver;
  aliceEndpoint: MemoryFipsEndpoint;
  bobEndpoint: MemoryFipsEndpoint;
  dispose(): Promise<void>;
}

function driverPair(
  overrides: Partial<FipsInvWantTcpDriverOptions> = {},
  identities = { alice: 'alice', bob: 'bob' },
): DriverPair {
  const aliceEndpoint = new MemoryFipsEndpoint(identities.alice);
  const bobEndpoint = new MemoryFipsEndpoint(identities.bob);
  aliceEndpoint.connect(bobEndpoint);
  bobEndpoint.connect(aliceEndpoint);
  const driverOptions = options(overrides);
  const alice = FipsInvWantTcpDriver.bind(
    aliceEndpoint,
    identities.alice,
    stream(),
    driverOptions,
    0xa11ce001n,
  );
  const bob = FipsInvWantTcpDriver.bind(
    bobEndpoint,
    identities.bob,
    stream(),
    driverOptions,
    0xb0b0e001n,
  );
  return {
    alice,
    bob,
    aliceEndpoint,
    bobEndpoint,
    dispose: async () => Promise.all([alice.dispose(), bob.dispose()]).then(() => undefined),
  };
}

function stream(): FipsInvWantStream {
  return new FipsInvWantStream({
    mesh: {
      maxEventBytes: 128 * 1024,
      maxCachedEventBytes: 512 * 1024,
    },
    maxRecordBytes: 132 * 1024,
  });
}

function options(
  overrides: Partial<FipsInvWantTcpDriverOptions> = {},
): FipsInvWantTcpDriverOptions {
  return {
    serviceNamespace: 'test.nostr.pubsub.stream',
    serviceVersion: 1,
    servicePort,
    maxPeers: 4,
    maxQueuedRecordsPerPeer: 64,
    maxQueuedBytesPerPeer: 512 * 1024,
    maxIoBytesPerDrive: 256 * 1024,
    ...overrides,
  };
}

interface PumpOutcome {
  aliceConnected: number;
  bobConnected: number;
  aliceStreamBytes: number;
  bobStreamBytes: number;
  aliceIds: Set<string>;
  bobIds: Set<string>;
}

async function pump(
  pair: DriverPair,
  startMs: number,
  complete: (outcome: PumpOutcome) => boolean,
): Promise<PumpOutcome> {
  const outcome: PumpOutcome = {
    aliceConnected: 0,
    bobConnected: 0,
    aliceStreamBytes: 0,
    bobStreamBytes: 0,
    aliceIds: new Set(),
    bobIds: new Set(),
  };
  for (let step = 0; step < 500; step += 1) {
    merge(outcome, 'alice', await pair.alice.receive(startMs + step));
    merge(outcome, 'bob', await pair.bob.receive(startMs + step));
    merge(outcome, 'alice', await pair.alice.poll(startMs + step));
    merge(outcome, 'bob', await pair.bob.poll(startMs + step));
    await tick();
    if (complete(outcome)) return outcome;
  }
  throw new Error(`driver pair did not converge: ${JSON.stringify({
    ...outcome,
    aliceIds: [...outcome.aliceIds],
    bobIds: [...outcome.bobIds],
  })}`);
}

function merge(
  outcome: PumpOutcome,
  side: 'alice' | 'bob',
  report: FipsInvWantTcpDriveReport,
): void {
  outcome[`${side}Connected`] = report.connectedPeers;
  outcome[`${side}StreamBytes`] += report.streamBytesRead;
  for (const delivery of report.deliveries) outcome[`${side}Ids`].add(delivery.event.id);
}

function signedEvent(content: string, keyByte: number): NostrVerifiedEvent {
  return verifyNostrEvent(finalizeEvent({
    kind: 1,
    created_at: 1_700_000_000 + keyByte,
    tags: [],
    content,
  }, new Uint8Array(32).fill(keyByte)));
}

function tick(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}
