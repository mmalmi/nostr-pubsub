import type { NostrEventSubscription, QueryEvent } from './event-bus.js';
import type { RuntimeSource } from './runtime-types.js';
import { type NostrEvent } from './types.js';
export type PublicPeerFeed = ({
    type: 'latest';
} | {
    type: 'following';
    owner: string;
    contacts: NostrEvent;
}) & {
    kinds?: readonly number[];
};
type Receive = (record: QueryEvent) => void;
/** A fixed background feed. Do not derive its configuration or lifetime from the open page. */
export declare function subscribePublicPeerFeed(source: RuntimeSource, feed: PublicPeerFeed, receive: Receive): Promise<NostrEventSubscription>;
/** Application queries have no peer history of their own. Explicit publication keeps its original receipt. */
export declare function publicPeerSource(source: RuntimeSource): RuntimeSource;
export interface PublicPeerFeedSource extends RuntimeSource {
    replaceFeed(feed: PublicPeerFeed): Promise<void>;
    close(): void;
}
/** One account-lifetime peer feed. All application filters match the bounded feed window locally. */
export declare function openPublicPeerFeedSource(source: RuntimeSource, feed: PublicPeerFeed): Promise<PublicPeerFeedSource>;
export {};
//# sourceMappingURL=public-peer-source.d.ts.map