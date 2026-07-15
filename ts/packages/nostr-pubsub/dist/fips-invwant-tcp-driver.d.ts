import { type FipsDatagramEndpoint } from '@fips/tcp';
import { FipsInvWantStream } from './fips-invwant-stream.js';
import { type FipsInvWantTcpDriveReport, type FipsInvWantTcpDriverOptions, type FipsInvWantTcpQueueSnapshot } from './fips-invwant-tcp-types.js';
import { type NostrVerifiedEvent } from './types.js';
/** Manually driven reliable Inv/WANT service over authenticated FIPS peers. */
export declare class FipsInvWantTcpDriver {
    private readonly stream;
    readonly options: FipsInvWantTcpDriverOptions;
    private readonly tcp;
    private readonly monitored;
    private readonly connections;
    private active;
    private readonly queues;
    private readonly localPeerOrderKey;
    private disposed;
    private constructor();
    static bind(endpoint: FipsDatagramEndpoint, localPeerId: string, stream: FipsInvWantStream, options: FipsInvWantTcpDriverOptions, isnSeed?: bigint | number): FipsInvWantTcpDriver;
    connectPeer(peer: string, nowMs?: number): Promise<void>;
    seed(event: NostrVerifiedEvent, nowMs: number): void;
    publish(event: NostrVerifiedEvent, nowMs: number): FipsInvWantTcpQueueSnapshot;
    receive(nowMs?: number): Promise<FipsInvWantTcpDriveReport>;
    poll(nowMs?: number): Promise<FipsInvWantTcpDriveReport>;
    abortPeer(peer: string): Promise<void>;
    connectedPeerCount(): number;
    queueSnapshot(): FipsInvWantTcpQueueSnapshot;
    dispose(): Promise<void>;
    private newReport;
    private driveReady;
    private acceptConnections;
    private refreshActive;
    private readActive;
    private flushQueues;
    private finishRemoteCloses;
    private applyActions;
    private ensurePeerCapacity;
    private ensureOpen;
}
//# sourceMappingURL=fips-invwant-tcp-driver.d.ts.map