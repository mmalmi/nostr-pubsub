import { PubsubError } from './types.js';
import type { InvWantWireMessage } from './mesh-codec.js';
import type { InvWantAction, UpstreamRoute } from './mesh-types.js';

export function requireEventId(eventId: string): void {
  if (!/^[0-9a-f]{64}$/.test(eventId)) throw validation(`invalid inv/want event id ${eventId}`);
}

export function requireKind(kind: number): number {
  if (!Number.isSafeInteger(kind) || kind < 0 || kind > 65_535) {
    throw validation(`invalid inv/want event kind ${kind}`);
  }
  return kind;
}

export function requireUnsignedByte(value: number, field: string): void {
  if (!Number.isSafeInteger(value) || value < 0 || value > 255) {
    throw validation(`invalid inv/want ${field} ${value}`);
  }
}

export function requireNow(value: number): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw validation(`invalid inv/want timestamp ${value}`);
  }
}

export function boundedPositive(value: number, maximum = Number.MAX_SAFE_INTEGER): number {
  if (!Number.isSafeInteger(value)) throw validation(`invalid positive integer ${value}`);
  return clamp(Math.max(1, value), 1, maximum);
}

export function nonNegative(value: number): number {
  if (!Number.isSafeInteger(value)) throw validation(`invalid non-negative integer ${value}`);
  return Math.max(0, value);
}

export function saturatingAdd(left: number, right: number): number {
  return Math.min(Number.MAX_SAFE_INTEGER, left + right);
}

export function clamp(value: number, minimum: number, maximum: number): number {
  return Math.max(minimum, Math.min(maximum, value));
}

export function retainMap<K, V>(map: Map<K, V>, predicate: (value: V, key: K) => boolean): void {
  for (const [key, value] of map) if (!predicate(value, key)) map.delete(key);
}

export function retainOrder<V>(order: string[], map: Map<string, V>): void {
  let write = 0;
  for (const id of order) if (map.has(id)) order[write++] = id;
  order.length = write;
}

export function validation(message: string): PubsubError {
  return PubsubError.validation(message);
}

export function send(peerId: string, message: InvWantWireMessage): InvWantAction {
  return { type: 'send', peerId, message };
}

export function routeHasProvider(route: UpstreamRoute, peerId: string): boolean {
  return route.peerId === peerId || route.alternatePeerIds.has(peerId);
}
