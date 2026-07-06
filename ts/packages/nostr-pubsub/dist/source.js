export const CAP_HASHTREE_FETCH = 'hashtree.fetch';
export const SOURCE_PRIORITY_LOCAL_INDEX = 300;
export const SOURCE_PRIORITY_FIPS_ENDPOINT = 200;
export const SOURCE_PRIORITY_PEER = 100;
export const SOURCE_PRIORITY_RELAY = -100;
export const EventSourceKind = {
    LocalIndex: 'local-index',
    Peer: 'peer',
    FipsEndpoint: 'fips-endpoint',
    Relay: 'relay',
};
export function sourceKindDefaultPriority(kind) {
    switch (kind) {
        case EventSourceKind.LocalIndex:
            return SOURCE_PRIORITY_LOCAL_INDEX;
        case EventSourceKind.FipsEndpoint:
            return SOURCE_PRIORITY_FIPS_ENDPOINT;
        case EventSourceKind.Peer:
            return SOURCE_PRIORITY_PEER;
        case EventSourceKind.Relay:
            return SOURCE_PRIORITY_RELAY;
    }
}
export function localIndexSource(id) {
    return { id, kind: EventSourceKind.LocalIndex };
}
export function peerSource(id) {
    return { id, kind: EventSourceKind.Peer };
}
export function fipsEndpointSource(id) {
    return { id, kind: EventSourceKind.FipsEndpoint };
}
export function relaySource(url) {
    return { id: url, kind: EventSourceKind.Relay, url };
}
//# sourceMappingURL=source.js.map