import type { NostrVerifiedEvent } from './types.js';
import type { FipsPubsubWireMessage } from './wire.js';
export interface FipsRemotePublication {
    eventId: string;
    provider: string;
}
export interface FipsReceiptOptions {
    timeoutMs?: number;
    signal?: AbortSignal;
}
/** One event per endpoint lifetime. A local queue/cache report is never a receipt. */
export declare class FipsPublicationReceipts {
    private used;
    private pending?;
    publish(event: NostrVerifiedEvent, peers: string[], send: (peer: string, message: FipsPubsubWireMessage) => Promise<void>, options: FipsReceiptOptions): Promise<FipsRemotePublication>;
    receive(peer: string, eventId: string, accepted: boolean, message: string): void;
    close(): void;
}
//# sourceMappingURL=fips-publication-receipts.d.ts.map