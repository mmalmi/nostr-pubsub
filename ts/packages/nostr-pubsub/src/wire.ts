import type { NostrEvent, NostrFilter, NostrVerifiedEvent, SourceId } from './types.js';
import { PubsubError, verifyNostrEvent } from './types.js';
import {
  PubsubPeerSubscriptionStore,
  type PubsubSubscriptionUpdate,
} from './subscription.js';

export const DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES = 64 * 1024;

export type FipsPubsubWireMessage =
  | { type: 'req'; subscriptionId: string; filters: NostrFilter[] }
  | { type: 'close'; subscriptionId: string }
  | { type: 'eose'; subscriptionId: string; eventCount: number }
  | { type: 'event'; event: NostrVerifiedEvent; subscriptionId?: string };

export class FipsPubsubWireCodec {
  readonly maxFrameBytes: number;

  constructor(maxFrameBytes = DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES) {
    if (!Number.isSafeInteger(maxFrameBytes) || maxFrameBytes <= 0) {
      throw PubsubError.validation('FIPS pubsub max frame bytes must be a positive safe integer');
    }
    this.maxFrameBytes = maxFrameBytes;
  }

  encodeFrame(message: FipsPubsubWireMessage): Uint8Array {
    const wireMessage = encodeWireMessage(message);
    const frame = new TextEncoder().encode(JSON.stringify(wireMessage));
    this.checkFrameSize(frame.byteLength);
    return frame;
  }

  decodeFrame(frame: Uint8Array): FipsPubsubWireMessage {
    this.checkFrameSize(frame.byteLength);
    if (frame.byteLength === 0) throw invalidFrame('frame is empty');

    let value: unknown;
    try {
      const json = new TextDecoder('utf-8', { fatal: true }).decode(frame);
      value = JSON.parse(json) as unknown;
    } catch (error) {
      throw invalidFrame(`invalid JSON: ${errorMessage(error)}`);
    }
    return decodeWireMessage(value);
  }

  private checkFrameSize(frameBytes: number): void {
    if (frameBytes > this.maxFrameBytes) {
      throw invalidFrame(`frame has ${frameBytes} bytes, limit is ${this.maxFrameBytes}`);
    }
  }
}

export interface FipsPubsubInbound {
  message: FipsPubsubWireMessage;
  subscriptionUpdate: PubsubSubscriptionUpdate;
}

export class FipsPubsubWireAdapter {
  constructor(
    readonly codec = new FipsPubsubWireCodec(),
    readonly subscriptions = new PubsubPeerSubscriptionStore(),
  ) {}

  decodeInbound(peerId: SourceId, frame: Uint8Array): FipsPubsubInbound {
    return this.applyInbound(peerId, this.codec.decodeFrame(frame));
  }

  applyInbound(peerId: SourceId, message: FipsPubsubWireMessage): FipsPubsubInbound {
    let subscriptionUpdate: PubsubSubscriptionUpdate = 'ignored';
    if (message.type === 'req') {
      this.subscriptions.upsertFilters(peerId, message.subscriptionId, message.filters);
      subscriptionUpdate = 'subscribed';
    } else if (message.type === 'close') {
      this.subscriptions.remove(peerId, message.subscriptionId);
      subscriptionUpdate = 'closed';
    }
    return { message, subscriptionUpdate };
  }

  encodeOutbound(message: FipsPubsubWireMessage): Uint8Array {
    return this.codec.encodeFrame(message);
  }
}

function encodeWireMessage(message: FipsPubsubWireMessage): unknown[] {
  switch (message.type) {
    case 'req':
      if (message.filters.length === 0) {
        throw invalidFrame('REQ requires at least one filter');
      }
      return ['REQ', message.subscriptionId, ...message.filters.map(normalizeFilter)];
    case 'close':
      return ['CLOSE', message.subscriptionId];
    case 'eose':
      if (!isNonNegativeSafeInteger(message.eventCount)) {
        throw invalidFrame('EOSE event count must be a non-negative safe integer');
      }
      return ['EOSE', message.subscriptionId, message.eventCount];
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

function decodeWireMessage(value: unknown): FipsPubsubWireMessage {
  if (!Array.isArray(value)) throw invalidFrame('message must be a JSON array');
  const [messageType] = value;
  if (typeof messageType !== 'string') throw invalidFrame('message type must be a string');

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
  if (messageType === 'EOSE') {
    if (
      value.length !== 3 ||
      typeof value[1] !== 'string' ||
      !isNonNegativeSafeInteger(value[2])
    ) {
      throw invalidFrame('EOSE requires an id and non-negative event count');
    }
    return { type: 'eose', subscriptionId: value[1], eventCount: value[2] };
  }
  if (messageType === 'EVENT') {
    if (value.length === 2) {
      return { type: 'event', event: verifyNostrEvent(value[1] as NostrEvent) };
    }
    if (value.length === 3 && typeof value[1] === 'string') {
      return {
        type: 'event',
        subscriptionId: value[1],
        event: verifyNostrEvent(value[2] as NostrEvent),
      };
    }
    throw invalidFrame('EVENT requires an event and optional subscription id');
  }
  throw invalidFrame(`unsupported Nostr message type ${messageType}`);
}

function normalizeFilter(value: unknown): NostrFilter {
  if (!isRecord(value)) throw invalidFrame('REQ filters must be JSON objects');
  const filter: NostrFilter = {};
  const knownKeys = new Set(['ids', 'authors', 'kinds', 'search', 'since', 'until', 'limit']);

  const tagKeys = Object.keys(value)
    .filter((key) => key.startsWith('#'))
    .sort();
  for (const key of tagKeys) {
    if (!/^#[A-Za-z]$/.test(key)) throw invalidFrame(`invalid generic filter tag ${key}`);
    filter[key as `#${string}`] = normalizeStringArray(value[key], key);
  }
  if ('ids' in value) filter.ids = normalizeHexArray(value.ids, 'ids');
  if ('authors' in value) filter.authors = normalizeHexArray(value.authors, 'authors');
  if ('kinds' in value) filter.kinds = normalizeIntegerArray(value.kinds, 'kinds');
  if ('search' in value) {
    if (typeof value.search !== 'string') throw invalidFrame('filter search must be a string');
    filter.search = value.search;
  }
  for (const key of ['since', 'until', 'limit'] as const) {
    if (!(key in value)) continue;
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
  return Object.fromEntries(
    Object.entries(filter).sort(([left], [right]) => compareUtf8(left, right)),
  ) as NostrFilter;
}

function normalizeStringArray(value: unknown, field: string): string[] {
  if (!Array.isArray(value) || value.some((item) => typeof item !== 'string')) {
    throw invalidFrame(`filter ${field} must be a string array`);
  }
  return [...new Set(value as string[])].sort(compareUtf8);
}

function normalizeHexArray(value: unknown, field: string): string[] {
  const values = normalizeStringArray(value, field);
  if (values.some((item) => !/^[0-9a-f]{64}$/.test(item))) {
    throw invalidFrame(`filter ${field} must contain 64-character lowercase hex values`);
  }
  return values;
}

function normalizeIntegerArray(value: unknown, field: string): number[] {
  if (
    !Array.isArray(value) ||
    value.some(
      (item) => !isNonNegativeSafeInteger(item) || (field === 'kinds' && item > 65_535),
    )
  ) {
    throw invalidFrame(`filter ${field} must be a non-negative integer array`);
  }
  return [...new Set(value as number[])].sort((left, right) => left - right);
}

function isNonNegativeSafeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function compareUtf8(left: string, right: string): number {
  const encoder = new TextEncoder();
  const leftBytes = encoder.encode(left);
  const rightBytes = encoder.encode(right);
  const sharedLength = Math.min(leftBytes.length, rightBytes.length);
  for (let index = 0; index < sharedLength; index += 1) {
    if (leftBytes[index] !== rightBytes[index]) return leftBytes[index] - rightBytes[index];
  }
  return leftBytes.length - rightBytes.length;
}

function invalidFrame(message: string): PubsubError {
  return PubsubError.validation(`invalid FIPS pubsub frame: ${message}`);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
