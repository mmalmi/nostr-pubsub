import type { SourceId } from './types.js';
export declare const DEFAULT_INV_WANT_HOP_LIMIT = 16;
export interface PubsubContentKey {
    streamId: string;
    origin: SourceId;
    seq: number;
}
export interface PubsubInventory {
    key: PubsubContentKey;
    payloadBytes: number;
    hopLimit: number;
}
export interface PubsubWant {
    key: PubsubContentKey;
}
export interface PubsubFrame {
    key: PubsubContentKey;
    payload: Uint8Array;
    hopLimit: number;
}
export type InvWantMessage = {
    type: 'inventory';
    inventory: PubsubInventory;
} | {
    type: 'want';
    want: PubsubWant;
} | {
    type: 'frame';
    frame: PubsubFrame;
};
export declare function createContentKey(streamId: string, origin: SourceId, seq: number): PubsubContentKey;
export declare function createInventory(key: PubsubContentKey, payloadBytes: number, hopLimit: number): PubsubInventory;
export declare function wantFromInventory(inventory: PubsubInventory): PubsubWant;
export declare function createWant(key: PubsubContentKey): PubsubWant;
export declare function createFrame(key: PubsubContentKey, payload: Uint8Array | ArrayLike<number>, hopLimit: number): PubsubFrame;
export declare function inventoryFromFrame(frame: PubsubFrame): PubsubInventory;
export declare function invWantMessageKey(message: InvWantMessage): PubsubContentKey;
export declare function invWantMessageStreamId(message: InvWantMessage): string;
//# sourceMappingURL=invwant.d.ts.map