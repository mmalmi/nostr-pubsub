import type {
  FipsDatagramEndpoint,
  FipsServiceContext,
} from '@fips/tcp';
import { npubEncode } from 'nostr-tools/nip19';
import type { QueryEvent } from './event-bus.js';
import { saturatingAdd } from './mesh-state.js';
import { PubsubError } from './types.js';

export interface FipsInvWantTcpDriverOptions {
  /** Authenticated FSP capability namespace, without its `/version` suffix. */
  serviceNamespace: string;
  serviceVersion: number;
  /** FSP service port and hidden TCP listener port. */
  servicePort: number;
  maxPeers: number;
  maxQueuedRecordsPerPeer: number;
  maxQueuedBytesPerPeer: number;
  maxIoBytesPerDrive: number;
}

export interface FipsInvWantTcpQueueSnapshot {
  peers: number;
  records: number;
  bytes: number;
}

export interface FipsInvWantTcpDriveReport {
  fipsDatagrams: number;
  rejectedTcpSegments: number;
  streamBytesRead: number;
  streamBytesWritten: number;
  connectedPeers: number;
  deliveries: QueryEvent[];
}

export function fipsInvWantTcpCapabilityName(
  options: Pick<FipsInvWantTcpDriverOptions, 'serviceNamespace' | 'serviceVersion'>,
): string {
  const namespace = options.serviceNamespace.trim().replace(/\/+$/, '');
  return `${namespace}/${options.serviceVersion}`;
}

/** Match Rust's npub ordering when TypeScript FIPS exposes a hex peer key. */
export function fipsInvWantTcpPeerOrderKey(peerId: string): string {
  const lowercase = peerId.toLowerCase();
  if (/^(02|03)[0-9a-f]{64}$/.test(lowercase)) return npubEncode(lowercase.slice(2));
  if (/^[0-9a-f]{64}$/.test(lowercase)) return npubEncode(lowercase);
  return lowercase.startsWith('npub1') ? lowercase : peerId;
}

export function validateFipsInvWantTcpOptions(options: FipsInvWantTcpDriverOptions): void {
  if (options.serviceNamespace.trim().replace(/\/+$/, '') === '') {
    throw validation('service namespace must not be empty');
  }
  unsigned(options.serviceVersion, 0xff, 'service version');
  unsigned(options.servicePort, 0xffff, 'service port', false);
  positive(options.maxPeers, 'max peers');
  positive(options.maxQueuedRecordsPerPeer, 'max queued records per peer');
  positive(options.maxQueuedBytesPerPeer, 'max queued bytes per peer');
  positive(options.maxIoBytesPerDrive, 'max I/O bytes per drive');
  if (!Number.isSafeInteger(options.maxPeers * 2)) {
    throw validation('TCP connection limit overflows');
  }
}

export class MonitoredFipsEndpoint implements FipsDatagramEndpoint {
  private datagrams = 0;
  private rejected = 0;

  constructor(private readonly endpoint: FipsDatagramEndpoint) {}

  registerService(
    port: number,
    handler: (context: FipsServiceContext) => Promise<void> | void,
  ): () => void {
    return this.endpoint.registerService(port, async (context) => {
      this.datagrams = saturatingAdd(this.datagrams, 1);
      try {
        await handler(context);
      } catch {
        // Match Rust receive_report: isolate malformed or over-capacity segments.
        this.rejected = saturatingAdd(this.rejected, 1);
      }
    });
  }

  sendDatagram(args: {
    dst: string;
    srcPort?: number;
    dstPort: number;
    payload: Uint8Array;
  }): Promise<void> {
    return this.endpoint.sendDatagram(args);
  }

  drainCounters(): Pick<FipsInvWantTcpDriveReport, 'fipsDatagrams' | 'rejectedTcpSegments'> {
    const counters = {
      fipsDatagrams: this.datagrams,
      rejectedTcpSegments: this.rejected,
    };
    this.datagrams = 0;
    this.rejected = 0;
    return counters;
  }
}

function positive(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw validation(`${name} must be greater than zero`);
  }
}

function unsigned(value: number, maximum: number, name: string, allowZero = true): void {
  if (!Number.isSafeInteger(value) || value < (allowZero ? 0 : 1) || value > maximum) {
    throw validation(`${name} is out of range`);
  }
}

function validation(message: string): PubsubError {
  return PubsubError.validation(message);
}
