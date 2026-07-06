export const DEFAULT_INV_WANT_HOP_LIMIT = 16;
export function createContentKey(streamId, origin, seq) {
    return { streamId, origin, seq };
}
export function createInventory(key, payloadBytes, hopLimit) {
    return { key: cloneContentKey(key), payloadBytes, hopLimit };
}
export function wantFromInventory(inventory) {
    return { key: cloneContentKey(inventory.key) };
}
export function createWant(key) {
    return { key: cloneContentKey(key) };
}
export function createFrame(key, payload, hopLimit) {
    return {
        key: cloneContentKey(key),
        payload: payload instanceof Uint8Array ? new Uint8Array(payload) : Uint8Array.from(payload),
        hopLimit,
    };
}
export function inventoryFromFrame(frame) {
    return createInventory(frame.key, frame.payload.byteLength, frame.hopLimit);
}
export function invWantMessageKey(message) {
    switch (message.type) {
        case 'inventory':
            return message.inventory.key;
        case 'want':
            return message.want.key;
        case 'frame':
            return message.frame.key;
    }
}
export function invWantMessageStreamId(message) {
    return invWantMessageKey(message).streamId;
}
function cloneContentKey(key) {
    return { streamId: key.streamId, origin: key.origin, seq: key.seq };
}
//# sourceMappingURL=invwant.js.map