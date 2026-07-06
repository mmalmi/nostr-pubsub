import type { Event, VerifiedEvent } from 'nostr-tools/core';
import type { Filter } from 'nostr-tools/filter';

export type NostrEvent = Event;
export type NostrFilter = Filter;
export type NostrVerifiedEvent = VerifiedEvent | Event;
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
