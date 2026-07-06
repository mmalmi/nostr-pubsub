import type { SourceId } from './types.js';

export const DEFAULT_INV_WANT_HOP_LIMIT = 16;

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

export type InvWantMessage =
  | { type: 'inventory'; inventory: PubsubInventory }
  | { type: 'want'; want: PubsubWant }
  | { type: 'frame'; frame: PubsubFrame };

export function createContentKey(streamId: string, origin: SourceId, seq: number): PubsubContentKey {
  return { streamId, origin, seq };
}

export function createInventory(
  key: PubsubContentKey,
  payloadBytes: number,
  hopLimit: number,
): PubsubInventory {
  return { key: cloneContentKey(key), payloadBytes, hopLimit };
}

export function wantFromInventory(inventory: PubsubInventory): PubsubWant {
  return { key: cloneContentKey(inventory.key) };
}

export function createWant(key: PubsubContentKey): PubsubWant {
  return { key: cloneContentKey(key) };
}

export function createFrame(
  key: PubsubContentKey,
  payload: Uint8Array | ArrayLike<number>,
  hopLimit: number,
): PubsubFrame {
  return {
    key: cloneContentKey(key),
    payload: payload instanceof Uint8Array ? new Uint8Array(payload) : Uint8Array.from(payload),
    hopLimit,
  };
}

export function inventoryFromFrame(frame: PubsubFrame): PubsubInventory {
  return createInventory(frame.key, frame.payload.byteLength, frame.hopLimit);
}

export function invWantMessageKey(message: InvWantMessage): PubsubContentKey {
  switch (message.type) {
    case 'inventory':
      return message.inventory.key;
    case 'want':
      return message.want.key;
    case 'frame':
      return message.frame.key;
  }
}

export function invWantMessageStreamId(message: InvWantMessage): string {
  return invWantMessageKey(message).streamId;
}

function cloneContentKey(key: PubsubContentKey): PubsubContentKey {
  return { streamId: key.streamId, origin: key.origin, seq: key.seq };
}
