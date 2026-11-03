import { FipsPubsubInvWantState } from './fips-pubsub-invwant.js';
import { FeedWindow } from './fips-feed-sync.js';
import { type FipsPubsubLocalSubscription } from './fips-pubsub-client-protocol.js';
import type { FipsNostrPubsubClientLimits } from './fips-pubsub-client-types.js';
import type { NostrFilter, NostrVerifiedEvent } from './types.js';
import type { FipsPubsubWireMessage } from './wire.js';
type Event = NostrVerifiedEvent;
type Inventory = Extract<FipsPubsubWireMessage, {
    type: 'inv';
}>;
export declare class FipsFeedSubscriptions {
    private readonly local;
    private readonly limits;
    private readonly canExport;
    private readonly send;
    private readonly drain;
    readonly wants: FipsPubsubInvWantState;
    private readonly sync;
    readonly sent: WeakSet<import("nostr-tools").VerifiedEvent>;
    private readonly published;
    private readonly remote;
    constructor(local: Map<string, FipsPubsubLocalSubscription>, limits: FipsNostrPubsubClientLimits, canExport: (e: Event) => boolean, send: (peer: string, message: FipsPubsubWireMessage) => void, drain: () => Promise<void>);
    publication(event: Event): import("nostr-tools").VerifiedEvent | undefined;
    lookup(peer: string, wireId: string): FipsPubsubLocalSubscription | undefined;
    validIds(peer: string, ids: string[], eventId: string): string[];
    deliver(peer: string, event: Event, offeredIds: readonly string[], onError: (e: unknown, id: string) => void): boolean;
    control(peer: string, message: Extract<FipsPubsubWireMessage, {
        type: 'sync';
    }>): Promise<void>;
    accepts(peer: string, id: string, filters: NostrFilter[]): boolean;
    offer(peer: string, id: string, filters: NostrFilter[]): Promise<void>;
    /** A cache hit from an unrelated local query can never authorize feed INV, WANT or forwarding. */
    inventory(peer: string, message: Inventory, fallback?: Event): {
        message: Inventory;
        event?: Event;
    } | undefined;
    answer(peer: string, eventId: string): {
        subscriptionId: string;
        event: import("nostr-tools").VerifiedEvent;
    } | undefined;
    retireRemote(peer: string, id: string): void;
    retireLocal(window: FeedWindow): void;
    disconnect(peer?: string): void;
}
export {};
//# sourceMappingURL=fips-feed-subscriptions.d.ts.map