import type { NostrEvent } from './types.js';
export declare const DEFAULT_INV_WANT_FANOUT = 8;
export declare const DEFAULT_INV_WANT_MAX_EVENT_BYTES: number;
export declare const DEFAULT_INV_WANT_MAX_WIRE_BYTES: number;
export type InvWantWireMessage = {
    type: 'inventory';
    eventId: string;
    eventKind: number;
    payloadBytes: number;
    hopLimit: number;
} | {
    type: 'want';
    eventId: string;
} | {
    type: 'frame';
    eventId: string;
    event: NostrEvent;
};
/** JSON envelope codec matching Rust's `InvWantCodec` byte-for-byte. */
export declare class InvWantCodec {
    readonly protocol: string;
    readonly version: number;
    readonly maxWireBytes: number;
    constructor(protocol: string, version: number, maxWireBytes: number);
    encode(message: InvWantWireMessage): Uint8Array;
    decode(payload: Uint8Array): InvWantWireMessage;
    private checkWireLength;
}
export declare function meshEventJsonBytes(event: NostrEvent): number;
//# sourceMappingURL=mesh-codec.d.ts.map