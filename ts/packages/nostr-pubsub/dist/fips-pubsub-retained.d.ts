import type { NostrEventReader } from './event-bus.js';
import { FipsPubsubEventCache } from './fips-pubsub-client-protocol.js';
import type { NostrEvent, NostrFilter, NostrVerifiedEvent } from './types.js';
import type { FipsPubsubWireMessage } from './wire.js';
/** Only queries application-owned retained storage; never discovers peers or fetches remotely. */
export declare class FipsRetainedReplay {
    private readonly cache;
    private readonly reader;
    private readonly limit;
    private readonly maxHops;
    private readonly admit;
    private readonly interested;
    private readonly send;
    private readonly pending;
    constructor(cache: FipsPubsubEventCache, reader: NostrEventReader | undefined, limit: number, maxHops: number, admit: (event: NostrEvent) => NostrVerifiedEvent, interested: (peer: string, subscription: string, event: NostrVerifiedEvent) => boolean, send: (peer: string, message: FipsPubsubWireMessage) => Promise<void>);
    replay(peer: string, subscription: string, filters: NostrFilter[]): Promise<void>;
    close(): void;
}
//# sourceMappingURL=fips-pubsub-retained.d.ts.map