import { verifiedSymbol, verifyEvent } from 'nostr-tools/pure';
const verifiedEventCopies = new WeakSet();
export function validateQueryOptions(options) {
    if (options.limit !== undefined &&
        (!Number.isSafeInteger(options.limit) || options.limit < 0)) {
        throw new RangeError('Query limit must be a non-negative safe integer');
    }
    if (options.deadline !== undefined && !Number.isFinite(options.deadline)) {
        throw new RangeError('Query deadline must be a finite Unix timestamp in milliseconds');
    }
}
export class PubsubError extends Error {
    kind;
    constructor(kind, message) {
        super(message);
        this.name = 'PubsubError';
        this.kind = kind;
    }
    static validation(message) {
        return new PubsubError('validation', message);
    }
    static storage(message) {
        return new PubsubError('storage', message);
    }
}
export function verifyNostrEvent(event) {
    const candidate = cloneNostrEvent(event);
    if (!verifyEvent(candidate)) {
        throw PubsubError.validation('invalid Nostr event id or signature');
    }
    return freezeVerifiedEvent(candidate);
}
/** Defensive immutable copy for an event already checked at a trust boundary. */
export function copyVerifiedNostrEvent(event) {
    if (!verifiedEventCopies.has(event)) {
        throw PubsubError.validation('verified mesh paths require verifyNostrEvent output');
    }
    const candidate = cloneNostrEvent(event);
    candidate[verifiedSymbol] = true;
    return freezeVerifiedEvent(candidate);
}
function cloneNostrEvent(event) {
    try {
        if (!Array.isArray(event.tags) ||
            event.tags.some((tag) => !Array.isArray(tag) || tag.some((item) => typeof item !== 'string'))) {
            throw new TypeError('invalid tags');
        }
        return {
            id: event.id,
            pubkey: event.pubkey,
            created_at: event.created_at,
            kind: event.kind,
            tags: event.tags.map((tag) => [...tag]),
            content: event.content,
            sig: event.sig,
        };
    }
    catch {
        throw PubsubError.validation('invalid Nostr event structure');
    }
}
function freezeVerifiedEvent(candidate) {
    if (!Number.isSafeInteger(candidate.created_at) ||
        candidate.created_at < 0 ||
        !Number.isSafeInteger(candidate.kind) ||
        candidate.kind < 0 ||
        candidate.kind > 65_535) {
        throw PubsubError.validation('invalid Nostr event timestamp or kind');
    }
    for (const tag of candidate.tags)
        Object.freeze(tag);
    Object.freeze(candidate.tags);
    verifiedEventCopies.add(candidate);
    return Object.freeze(candidate);
}
//# sourceMappingURL=types.js.map