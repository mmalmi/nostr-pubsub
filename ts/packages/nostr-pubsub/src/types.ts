import type { Event, VerifiedEvent } from 'nostr-tools/core';
import type { Filter } from 'nostr-tools/filter';
import { verifyEvent } from 'nostr-tools/pure';

export type NostrEvent = Event;
export type NostrFilter = Filter;
export type NostrVerifiedEvent = VerifiedEvent;
export type SourceId = string;

export interface QueryOptions {
  limit?: number;
}

export class PubsubError extends Error {
  readonly kind: 'validation' | 'storage';

  private constructor(kind: 'validation' | 'storage', message: string) {
    super(message);
    this.name = 'PubsubError';
    this.kind = kind;
  }

  static validation(message: string): PubsubError {
    return new PubsubError('validation', message);
  }

  static storage(message: string): PubsubError {
    return new PubsubError('storage', message);
  }
}

export function verifyNostrEvent(event: NostrEvent): NostrVerifiedEvent {
  let candidate: NostrEvent;
  try {
    if (
      !Array.isArray(event.tags) ||
      event.tags.some(
        (tag) => !Array.isArray(tag) || tag.some((item) => typeof item !== 'string'),
      )
    ) {
      throw new TypeError('invalid tags');
    }
    candidate = {
      id: event.id,
      pubkey: event.pubkey,
      created_at: event.created_at,
      kind: event.kind,
      tags: event.tags.map((tag) => [...tag]),
      content: event.content,
      sig: event.sig,
    };
  } catch {
    throw PubsubError.validation('invalid Nostr event structure');
  }

  if (
    !Number.isSafeInteger(candidate.created_at) ||
    candidate.created_at < 0 ||
    !Number.isSafeInteger(candidate.kind) ||
    candidate.kind < 0 ||
    candidate.kind > 65_535
  ) {
    throw PubsubError.validation('invalid Nostr event timestamp or kind');
  }
  if (!verifyEvent(candidate)) {
    throw PubsubError.validation('invalid Nostr event id or signature');
  }

  for (const tag of candidate.tags) Object.freeze(tag);
  Object.freeze(candidate.tags);
  return Object.freeze(candidate);
}
