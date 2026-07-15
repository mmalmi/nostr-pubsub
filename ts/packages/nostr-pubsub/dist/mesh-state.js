import { PubsubError } from './types.js';
export function requireEventId(eventId) {
    if (!/^[0-9a-f]{64}$/.test(eventId))
        throw validation(`invalid inv/want event id ${eventId}`);
}
export function requireKind(kind) {
    if (!Number.isSafeInteger(kind) || kind < 0 || kind > 65_535) {
        throw validation(`invalid inv/want event kind ${kind}`);
    }
    return kind;
}
export function requireUnsignedByte(value, field) {
    if (!Number.isSafeInteger(value) || value < 0 || value > 255) {
        throw validation(`invalid inv/want ${field} ${value}`);
    }
}
export function requireNow(value) {
    if (!Number.isSafeInteger(value) || value < 0) {
        throw validation(`invalid inv/want timestamp ${value}`);
    }
}
export function boundedPositive(value, maximum = Number.MAX_SAFE_INTEGER) {
    if (!Number.isSafeInteger(value))
        throw validation(`invalid positive integer ${value}`);
    return clamp(Math.max(1, value), 1, maximum);
}
export function nonNegative(value) {
    if (!Number.isSafeInteger(value))
        throw validation(`invalid non-negative integer ${value}`);
    return Math.max(0, value);
}
export function saturatingAdd(left, right) {
    return Math.min(Number.MAX_SAFE_INTEGER, left + right);
}
export function clamp(value, minimum, maximum) {
    return Math.max(minimum, Math.min(maximum, value));
}
export function retainMap(map, predicate) {
    for (const [key, value] of map)
        if (!predicate(value, key))
            map.delete(key);
}
export function retainOrder(order, map) {
    let write = 0;
    for (const id of order)
        if (map.has(id))
            order[write++] = id;
    order.length = write;
}
export function validation(message) {
    return PubsubError.validation(message);
}
export function send(peerId, message) {
    return { type: 'send', peerId, message };
}
export function routeHasProvider(route, peerId) {
    return route.peerId === peerId || route.alternatePeerIds.has(peerId);
}
//# sourceMappingURL=mesh-state.js.map