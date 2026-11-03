import type { NostrFilter, NostrVerifiedEvent } from './types.js';
import type { FipsPubsubWireMessage } from './wire.js';
export declare const warmFeedId: RegExp;
export declare const publicFeedId: RegExp;
type Sync = Extract<FipsPubsubWireMessage, {
    type: 'sync';
}>;
type Event = NostrVerifiedEvent;
/** Owned by one explicit public carrier subscription. Never seeded from a general event cache. */
export declare class FeedWindow {
    readonly filter: NostrFilter;
    readonly limit: number;
    private readonly records;
    constructor(filter: NostrFilter, limit: number);
    remember(event: Event): void;
    accepts(event: Event): boolean;
    get(id: string): import("nostr-tools").VerifiedEvent | undefined;
    snapshot(): Event[];
}
/** Provider initiates so only its verified, missing events are offered through ordinary INV/WANT. */
export declare class FeedSync {
    private readonly send;
    private readonly maxFrame;
    private readonly maxSessions;
    private readonly offering;
    private readonly receiving;
    constructor(send: (peer: string, msg: Sync) => void, maxFrame: number, maxSessions: number);
    private start;
    private take;
    private frame;
    offer(peer: string, id: string, owner: FeedWindow, inventory: Event[], finish: (events: Event[], fallback?: boolean) => void): Promise<void>;
    receive(peer: string, message: Sync, local: FeedWindow | undefined, allowOpen: () => boolean): Promise<void>;
    cancel(peer?: string, id?: string, owner?: FeedWindow, direction?: 'offer'): void;
}
export {};
//# sourceMappingURL=fips-feed-sync.d.ts.map