import type { FipsDatagramEndpoint, FipsServiceContext } from '@fips/tcp';
import type { QueryEvent } from './event-bus.js';
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
export declare function fipsInvWantTcpCapabilityName(options: Pick<FipsInvWantTcpDriverOptions, 'serviceNamespace' | 'serviceVersion'>): string;
/** Match Rust's npub ordering when TypeScript FIPS exposes a hex peer key. */
export declare function fipsInvWantTcpPeerOrderKey(peerId: string): string;
export declare function validateFipsInvWantTcpOptions(options: FipsInvWantTcpDriverOptions): void;
export declare class MonitoredFipsEndpoint implements FipsDatagramEndpoint {
    private readonly endpoint;
    private datagrams;
    private rejected;
    constructor(endpoint: FipsDatagramEndpoint);
    registerService(port: number, handler: (context: FipsServiceContext) => Promise<void> | void): () => void;
    sendDatagram(args: {
        dst: string;
        srcPort?: number;
        dstPort: number;
        payload: Uint8Array;
    }): Promise<void>;
    drainCounters(): Pick<FipsInvWantTcpDriveReport, 'fipsDatagrams' | 'rejectedTcpSegments'>;
}
//# sourceMappingURL=fips-invwant-tcp-types.d.ts.map