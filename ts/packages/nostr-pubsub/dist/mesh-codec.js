import { PubsubError } from './types.js';
export const DEFAULT_INV_WANT_FANOUT = 8;
export const DEFAULT_INV_WANT_MAX_EVENT_BYTES = 1024 * 1024;
export const DEFAULT_INV_WANT_MAX_WIRE_BYTES = DEFAULT_INV_WANT_MAX_EVENT_BYTES + 4096;
/** JSON envelope codec matching Rust's `InvWantCodec` byte-for-byte. */
export class InvWantCodec {
    protocol;
    version;
    maxWireBytes;
    constructor(protocol, version, maxWireBytes) {
        if (!Number.isInteger(version) || version < 0 || version > 255) {
            throw validation('inv/want version must be an unsigned byte');
        }
        this.protocol = protocol;
        this.version = version;
        this.maxWireBytes = Math.max(1, requireSafeInteger(maxWireBytes, 'max wire bytes'));
    }
    encode(message) {
        const encoded = new TextEncoder().encode(JSON.stringify({
            protocol: this.protocol,
            version: this.version,
            message: messageToWire(message),
        }));
        this.checkWireLength(encoded.byteLength);
        return encoded;
    }
    decode(payload) {
        this.checkWireLength(payload.byteLength);
        let value;
        try {
            value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(payload));
        }
        catch (error) {
            throw validation(`invalid inv/want JSON: ${errorMessage(error)}`);
        }
        if (!isRecord(value))
            throw validation('invalid inv/want JSON: envelope must be an object');
        if (value.protocol !== this.protocol) {
            throw validation(`unsupported inv/want protocol ${JSON.stringify(value.protocol)}`);
        }
        if (value.version !== this.version) {
            throw validation(`unsupported inv/want version ${String(value.version)}`);
        }
        return messageFromWire(value.message);
    }
    checkWireLength(length) {
        if (length > this.maxWireBytes) {
            throw validation(`inv/want wire payload is ${length} bytes, maximum is ${this.maxWireBytes}`);
        }
    }
}
export function meshEventJsonBytes(event) {
    return new TextEncoder().encode(JSON.stringify(eventToWire(event))).byteLength;
}
function messageToWire(message) {
    switch (message.type) {
        case 'inventory':
            return {
                type: 'inventory',
                event_id: requireEventId(message.eventId),
                event_kind: requireU16(message.eventKind, 'event kind'),
                payload_bytes: requireU32(message.payloadBytes, 'payload bytes'),
                hop_limit: requireU8(message.hopLimit, 'hop limit'),
            };
        case 'want':
            return { type: 'want', event_id: requireEventId(message.eventId) };
        case 'frame':
            return {
                type: 'frame',
                event_id: requireEventId(message.eventId),
                event: eventToWire(message.event),
            };
    }
}
function messageFromWire(value) {
    if (!isRecord(value))
        throw validation('invalid inv/want JSON: message must be an object');
    switch (value.type) {
        case 'inventory':
            return {
                type: 'inventory',
                eventId: requireEventId(value.event_id),
                eventKind: requireU16(value.event_kind, 'event kind'),
                payloadBytes: requireU32(value.payload_bytes, 'payload bytes'),
                hopLimit: requireU8(value.hop_limit, 'hop limit'),
            };
        case 'want':
            return { type: 'want', eventId: requireEventId(value.event_id) };
        case 'frame':
            return {
                type: 'frame',
                eventId: requireEventId(value.event_id),
                event: eventFromWire(value.event),
            };
        default:
            throw validation(`invalid inv/want message type ${String(value.type)}`);
    }
}
function eventToWire(event) {
    const parsed = eventFromWire(event);
    return {
        id: parsed.id,
        pubkey: parsed.pubkey,
        created_at: parsed.created_at,
        kind: parsed.kind,
        tags: parsed.tags,
        content: parsed.content,
        sig: parsed.sig,
    };
}
function eventFromWire(value) {
    if (!isRecord(value))
        throw validation('invalid inv/want event structure');
    const tags = value.tags;
    if (!Array.isArray(tags) ||
        tags.some((tag) => !Array.isArray(tag) || tag.some((part) => typeof part !== 'string'))) {
        throw validation('invalid inv/want event tags');
    }
    if (typeof value.id !== 'string' ||
        !/^[0-9a-fA-F]{64}$/.test(value.id) ||
        typeof value.pubkey !== 'string' ||
        !/^[0-9a-fA-F]{64}$/.test(value.pubkey) ||
        typeof value.sig !== 'string' ||
        !/^[0-9a-fA-F]{128}$/.test(value.sig) ||
        typeof value.content !== 'string') {
        throw validation('invalid inv/want event structure');
    }
    return {
        id: value.id,
        pubkey: value.pubkey,
        created_at: requireNonNegativeSafeInteger(value.created_at, 'event timestamp'),
        kind: requireU16(value.kind, 'event kind'),
        tags: tags.map((tag) => [...tag]),
        content: value.content,
        sig: value.sig,
    };
}
function requireEventId(value) {
    if (typeof value !== 'string' || !/^[0-9a-fA-F]{64}$/.test(value)) {
        throw validation(`invalid inv/want event id ${String(value)}`);
    }
    return value;
}
function requireU8(value, field) {
    const number = requireNonNegativeSafeInteger(value, field);
    if (number > 255)
        throw validation(`invalid inv/want ${field} ${number}`);
    return number;
}
function requireU16(value, field) {
    const number = requireNonNegativeSafeInteger(value, field);
    if (number > 65_535)
        throw validation(`invalid inv/want ${field} ${number}`);
    return number;
}
function requireU32(value, field) {
    const number = requireNonNegativeSafeInteger(value, field);
    if (number > 0xffff_ffff)
        throw validation(`invalid inv/want ${field} ${number}`);
    return number;
}
function requireNonNegativeSafeInteger(value, field) {
    if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
        throw validation(`invalid inv/want ${field} ${String(value)}`);
    }
    return value;
}
function requireSafeInteger(value, field) {
    if (typeof value !== 'number' || !Number.isSafeInteger(value)) {
        throw validation(`invalid inv/want ${field} ${String(value)}`);
    }
    return value;
}
function isRecord(value) {
    return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function errorMessage(error) {
    return error instanceof Error ? error.message : String(error);
}
function validation(message) {
    return PubsubError.validation(message);
}
//# sourceMappingURL=mesh-codec.js.map