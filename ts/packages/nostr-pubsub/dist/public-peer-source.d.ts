import type { NostrEventSubscription, QueryEvent } from './event-bus.js';
import type { RuntimeSource } from './runtime-types.js';
import { type NostrEvent, type NostrFilter } from './types.js';
export type PublicPeerFeed = ({
    type: 'latest';
} | {
    type: 'notifications';
    owner: string;
} | {
    type: 'following';
    owner: string;
    contacts: NostrEvent;
}) & {
    kinds?: readonly number[];
};
type Receive = (record: QueryEvent) => void;
export declare function privatePeerEvent(event: NostrEvent): boolean;
export declare function validPublicFeedFilter(filter: NostrFilter): boolean;
/** A fixed background feed. Do not derive its configuration or lifetime from the open page. */
export declare function subscribePublicPeerFeed(source: RuntimeSource, feed: PublicPeerFeed, receive: Receive): Promise<NostrEventSubscription>;
/** Internal carrier binding. Only feedFilters can construct an account-lifetime subscription. */
export declare const publicFeedSubscribe: unique symbol;
export interface PublicFeedCarrier {
    [publicFeedSubscribe]?(filter: NostrFilter, receive: Receive): NostrEventSubscription;
}
/** Application queries have no peer history of their own. Explicit publication keeps its original receipt. */
export declare function publicPeerSource(source: RuntimeSource): RuntimeSource;
type FeedSet = PublicPeerFeed | readonly PublicPeerFeed[];
export interface PublicPeerFeedSource extends RuntimeSource {
    replaceFeed(feed: FeedSet): Promise<void>;
    close(): void;
}
/** One account-lifetime source. Overlapping feeds share a bounded local display window. */
export declare function openPublicPeerFeedSource(source: RuntimeSource, feed: FeedSet): Promise<PublicPeerFeedSource>;
export {};
//# sourceMappingURL=public-peer-source.d.ts.map