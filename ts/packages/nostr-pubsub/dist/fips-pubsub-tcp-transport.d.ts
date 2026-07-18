import { type FipsDatagramEndpoint } from '@fips/tcp';
export interface FipsPubsubTcpTransportOptions {
    servicePort: number;
    maxPeers: number;
    maxFrameBytes: number;
    maxQueuedRecordsPerPeer: number;
    maxQueuedBytesPerPeer: number;
    maxIoBytesPerDrive: number;
    maxFramesPerDrive: number;
}
export interface FipsPubsubTcpTransportCallbacks {
    frame(peerId: string, frame: Uint8Array): void;
    connected(peerId: string): void;
    disconnected(peerId: string): void;
    tick(nowMs: number): void;
    error(error: Error): void;
}
/** Reliable record transport shared with Rust's `WireTcpDriver`. */
export declare class FipsPubsubTcpTransport {
    readonly options: FipsPubsubTcpTransportOptions;
    private readonly callbacks;
    private readonly tcp;
    private readonly connections;
    private active;
    private readonly queues;
    private readonly inputs;
    private readonly localPeerOrderKey;
    private operation;
    private timer?;
    private disposed;
    constructor(endpoint: FipsDatagramEndpoint, localPeerId: string, options: FipsPubsubTcpTransportOptions, callbacks: FipsPubsubTcpTransportCallbacks, isnSeed?: bigint | number);
    connectPeer(peer: string, nowMs?: number): Promise<void>;
    queueFrame(peerId: string, frame: Uint8Array): void;
    abortPeer(peer: string): Promise<void>;
    connectedPeerCount(): number;
    isConnected(peerId: string): boolean;
    idle(): Promise<void>;
    dispose(): Promise<void>;
    private scheduleDrive;
    private enqueueDrive;
    private driveReady;
    private acceptConnections;
    private refreshActive;
    private readActive;
    private flushQueues;
    private finishRemoteCloses;
    private armTimer;
    private ensurePeerCapacity;
    private ensureOpen;
}
//# sourceMappingURL=fips-pubsub-tcp-transport.d.ts.map