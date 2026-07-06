import type { SourceId } from './types.js';
export declare const CAP_HASHTREE_FETCH = "hashtree.fetch";
export declare const SOURCE_PRIORITY_LOCAL_INDEX = 300;
export declare const SOURCE_PRIORITY_FIPS_ENDPOINT = 200;
export declare const SOURCE_PRIORITY_PEER = 100;
export declare const SOURCE_PRIORITY_RELAY = -100;
export declare const EventSourceKind: {
    readonly LocalIndex: "local-index";
    readonly Peer: "peer";
    readonly FipsEndpoint: "fips-endpoint";
    readonly Relay: "relay";
};
export type EventSourceKind = (typeof EventSourceKind)[keyof typeof EventSourceKind];
export interface EventSource {
    id: SourceId;
    kind: EventSourceKind;
    url?: string;
}
export declare function sourceKindDefaultPriority(kind: EventSourceKind): number;
export declare function localIndexSource(id: SourceId): EventSource;
export declare function peerSource(id: SourceId): EventSource;
export declare function fipsEndpointSource(id: SourceId): EventSource;
export declare function relaySource(url: string): EventSource;
//# sourceMappingURL=source.d.ts.map