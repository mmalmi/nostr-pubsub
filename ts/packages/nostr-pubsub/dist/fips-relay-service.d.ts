import { type NostrEvent, type NostrFilter, type NostrVerifiedEvent } from './types.js';
import { FipsPubsubWireAdapter } from './wire.js';
export declare const FIPS_NOSTR_PUBSUB_SERVICE_PORT = 7368;
export declare const FIPS_NOSTR_PUBSUB_MAX_REPLAY_EVENTS = 8;
export interface FipsPubsubServiceContext {
    src: string;
    srcPort: number;
    dstPort: number;
    payload: Uint8Array;
    reply: (data: Uint8Array, replyDstPort?: number) => Promise<void>;
}
export type FipsPubsubServiceHandler = (context: FipsPubsubServiceContext) => Promise<void> | void;
export interface FipsPubsubServiceNode {
    registerService(port: number, handler: FipsPubsubServiceHandler): () => void;
    on?(event: 'session', listener: (event: unknown) => void): () => void;
}
export interface NostrRelaySubscription {
    close(reason?: string): void;
}
export interface NostrRelayTransportHandlers {
    onEvent(event: NostrEvent): void;
    onEose?(): void;
    onClose?(reasons?: readonly string[]): void;
}
export interface NostrRelayTransport {
    subscribe(filters: NostrFilter[], handlers: NostrRelayTransportHandlers): NostrRelaySubscription;
    publish(event: NostrVerifiedEvent): Promise<void> | void;
}
export interface FipsNostrRelayServiceLimits {
    maxPeers: number;
    maxSubscriptionsPerPeer: number;
    maxFiltersPerSubscription: number;
    maxReplayEventsPerFilter: number;
    subscriptionTtlMs: number;
    maxFrameBytes: number;
}
export interface FipsNostrRelayServiceErrorContext {
    operation: 'relay-event' | 'relay-eose' | 'subscription-close';
    peerId: string;
    subscriptionId: string;
}
export interface FipsNostrRelayServiceOptions {
    node: FipsPubsubServiceNode;
    relay: NostrRelayTransport;
    limits?: Partial<FipsNostrRelayServiceLimits>;
    onError?: (error: Error, context: FipsNostrRelayServiceErrorContext) => void;
}
export declare function defaultFipsNostrRelayServiceLimits(): FipsNostrRelayServiceLimits;
export declare class FipsNostrRelayService {
    readonly adapter: FipsPubsubWireAdapter;
    readonly limits: FipsNostrRelayServiceLimits;
    private readonly node;
    private readonly relay;
    private readonly onError;
    private readonly peers;
    private readonly pendingReplies;
    private unregisterService?;
    private removeSessionListener?;
    constructor(options: FipsNostrRelayServiceOptions);
    start(): this;
    stop(): Promise<void>;
    activePeerCount(): number;
    activeSubscriptionCount(): number;
    peerSubscriptionCount(peerId: string): number;
    closePeer(peerId: string): void;
    private handle;
    private openSubscription;
    private queueRelayEvent;
    private queueRelayEose;
    private queueReply;
    private closeSubscription;
}
//# sourceMappingURL=fips-relay-service.d.ts.map