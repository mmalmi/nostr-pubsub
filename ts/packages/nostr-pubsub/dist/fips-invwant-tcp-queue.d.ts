import type { FipsInvWantTcpDriverOptions, FipsInvWantTcpQueueSnapshot } from './fips-invwant-tcp-types.js';
export interface PendingInvWantRecord {
    peerId: string;
    record: Uint8Array;
}
export declare class InvWantRecordQueues {
    private readonly options;
    private readonly queues;
    constructor(options: FipsInvWantTcpDriverOptions);
    snapshot(): FipsInvWantTcpQueueSnapshot;
    enqueue(records: readonly PendingInvWantRecord[]): void;
    nextChunk(peerId: string, maximum: number): Uint8Array | undefined;
    accept(peerId: string, bytes: number): void;
    has(peerId: string): boolean;
    delete(peerId: string): void;
    restart(peerId: string): void;
    clear(): void;
}
//# sourceMappingURL=fips-invwant-tcp-queue.d.ts.map