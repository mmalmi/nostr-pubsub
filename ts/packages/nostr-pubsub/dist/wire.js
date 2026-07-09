import { PubsubError, verifyNostrEvent } from './types.js';
import { PubsubPeerSubscriptionStore, } from './subscription.js';
export const DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES = 64 * 1024;
export class FipsPubsubWireCodec {
    maxFrameBytes;
    constructor(maxFrameBytes = DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES) {
        if (!Number.isSafeInteger(maxFrameBytes) || maxFrameBytes <= 0) {
            throw PubsubError.validation('FIPS pubsub max frame bytes must be a positive safe integer');
        }
        this.maxFrameBytes = maxFrameBytes;
    }
    encodeFrame(message) {
        const wireMessage = encodeWireMessage(message);
        const frame = new TextEncoder().encode(JSON.stringify(wireMessage));
        this.checkFrameSize(frame.byteLength);
        return frame;
    }
    decodeFrame(frame) {
        this.checkFrameSize(frame.byteLength);
        if (frame.byteLength === 0)
            throw invalidFrame('frame is empty');
        let value;
        try {
            const json = new TextDecoder('utf-8', { fatal: true }).decode(frame);
            value = JSON.parse(json);
        }
        catch (error) {
            throw invalidFrame(`invalid JSON: ${errorMessage(error)}`);
        }
        return decodeWireMessage(value);
    }
    checkFrameSize(frameBytes) {
        if (frameBytes > this.maxFrameBytes) {
            throw invalidFrame(`frame has ${frameBytes} bytes, limit is ${this.maxFrameBytes}`);
        }
    }
}
export class FipsPubsubWireAdapter {
    codec;
    subscriptions;
    constructor(codec = new FipsPubsubWireCodec(), subscriptions = new PubsubPeerSubscriptionStore()) {
        this.codec = codec;
        this.subscriptions = subscriptions;
    }
    decodeInbound(peerId, frame) {
        const message = this.codec.decodeFrame(frame);
        let subscriptionUpdate = 'ignored';
        if (message.type === 'req') {
            this.subscriptions.upsertFilters(peerId, message.subscriptionId, message.filters);
            subscriptionUpdate = 'subscribed';
        }
        else if (message.type === 'close') {
            this.subscriptions.remove(peerId, message.subscriptionId);
            subscriptionUpdate = 'closed';
        }
        return { message, subscriptionUpdate };
    }
    encodeOutbound(message) {
        return this.codec.encodeFrame(message);
    }
}
function encodeWireMessage(message) {
    switch (message.type) {
        case 'req':
            if (message.filters.length === 0) {
                throw invalidFrame('REQ requires at least one filter');
            }
            return ['REQ', message.subscriptionId, ...message.filters.map(normalizeFilter)];
        case 'close':
            return ['CLOSE', message.subscriptionId];
        case 'event': {
            const event = verifyNostrEvent(message.event);
            const wireEvent = {
                content: event.content,
                created_at: event.created_at,
                id: event.id,
                kind: event.kind,
                pubkey: event.pubkey,
                sig: event.sig,
                tags: event.tags,
            };
            return message.subscriptionId === undefined
                ? ['EVENT', wireEvent]
                : ['EVENT', message.subscriptionId, wireEvent];
        }
    }
}
function decodeWireMessage(value) {
    if (!Array.isArray(value))
        throw invalidFrame('message must be a JSON array');
    const [messageType] = value;
    if (typeof messageType !== 'string')
        throw invalidFrame('message type must be a string');
    if (messageType === 'REQ') {
        if (value.length < 3 || typeof value[1] !== 'string') {
            throw invalidFrame('REQ requires an id and at least one filter');
        }
        return {
            type: 'req',
            subscriptionId: value[1],
            filters: value.slice(2).map(normalizeFilter),
        };
    }
    if (messageType === 'CLOSE') {
        if (value.length !== 2 || typeof value[1] !== 'string') {
            throw invalidFrame('CLOSE requires exactly an id');
        }
        return { type: 'close', subscriptionId: value[1] };
    }
    if (messageType === 'EVENT') {
        if (value.length === 2) {
            return { type: 'event', event: verifyNostrEvent(value[1]) };
        }
        if (value.length === 3 && typeof value[1] === 'string') {
            return {
                type: 'event',
                subscriptionId: value[1],
                event: verifyNostrEvent(value[2]),
            };
        }
        throw invalidFrame('EVENT requires an event and optional subscription id');
    }
    throw invalidFrame(`unsupported Nostr message type ${messageType}`);
}
function normalizeFilter(value) {
    if (!isRecord(value))
        throw invalidFrame('REQ filters must be JSON objects');
    const filter = {};
    const knownKeys = new Set(['ids', 'authors', 'kinds', 'search', 'since', 'until', 'limit']);
    const tagKeys = Object.keys(value)
        .filter((key) => key.startsWith('#'))
        .sort();
    for (const key of tagKeys) {
        if (!/^#[A-Za-z]$/.test(key))
            throw invalidFrame(`invalid generic filter tag ${key}`);
        filter[key] = normalizeStringArray(value[key], key);
    }
    if ('ids' in value)
        filter.ids = normalizeHexArray(value.ids, 'ids');
    if ('authors' in value)
        filter.authors = normalizeHexArray(value.authors, 'authors');
    if ('kinds' in value)
        filter.kinds = normalizeIntegerArray(value.kinds, 'kinds');
    if ('search' in value) {
        if (typeof value.search !== 'string')
            throw invalidFrame('filter search must be a string');
        filter.search = value.search;
    }
    for (const key of ['since', 'until', 'limit']) {
        if (!(key in value))
            continue;
        const number = value[key];
        if (!isNonNegativeSafeInteger(number)) {
            throw invalidFrame(`filter ${key} must be a non-negative safe integer`);
        }
        filter[key] = number;
    }
    for (const key of Object.keys(value)) {
        if (!knownKeys.has(key) && !key.startsWith('#')) {
            throw invalidFrame(`unsupported filter field ${key}`);
        }
    }
    return Object.fromEntries(Object.entries(filter).sort(([left], [right]) => compareUtf8(left, right)));
}
function normalizeStringArray(value, field) {
    if (!Array.isArray(value) || value.some((item) => typeof item !== 'string')) {
        throw invalidFrame(`filter ${field} must be a string array`);
    }
    return [...new Set(value)].sort(compareUtf8);
}
function normalizeHexArray(value, field) {
    const values = normalizeStringArray(value, field);
    if (values.some((item) => !/^[0-9a-f]{64}$/.test(item))) {
        throw invalidFrame(`filter ${field} must contain 64-character lowercase hex values`);
    }
    return values;
}
function normalizeIntegerArray(value, field) {
    if (!Array.isArray(value) ||
        value.some((item) => !isNonNegativeSafeInteger(item) || (field === 'kinds' && item > 65_535))) {
        throw invalidFrame(`filter ${field} must be a non-negative integer array`);
    }
    return [...new Set(value)].sort((left, right) => left - right);
}
function isNonNegativeSafeInteger(value) {
    return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}
function isRecord(value) {
    return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function compareUtf8(left, right) {
    const encoder = new TextEncoder();
    const leftBytes = encoder.encode(left);
    const rightBytes = encoder.encode(right);
    const sharedLength = Math.min(leftBytes.length, rightBytes.length);
    for (let index = 0; index < sharedLength; index += 1) {
        if (leftBytes[index] !== rightBytes[index])
            return leftBytes[index] - rightBytes[index];
    }
    return leftBytes.length - rightBytes.length;
}
function invalidFrame(message) {
    return PubsubError.validation(`invalid FIPS pubsub frame: ${message}`);
}
function errorMessage(error) {
    return error instanceof Error ? error.message : String(error);
}
//# sourceMappingURL=wire.js.map