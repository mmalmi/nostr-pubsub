import { PubsubError } from './types.js';
import type { InvWantWireMessage } from './mesh-codec.js';
import type { InvWantAction, UpstreamRoute } from './mesh-types.js';
export declare function requireEventId(eventId: string): void;
export declare function requireKind(kind: number): number;
export declare function requireUnsignedByte(value: number, field: string): void;
export declare function requireNow(value: number): void;
export declare function boundedPositive(value: number, maximum?: number): number;
export declare function nonNegative(value: number): number;
export declare function saturatingAdd(left: number, right: number): number;
export declare function clamp(value: number, minimum: number, maximum: number): number;
export declare function retainMap<K, V>(map: Map<K, V>, predicate: (value: V, key: K) => boolean): void;
export declare function retainOrder<V>(order: string[], map: Map<string, V>): void;
export declare function validation(message: string): PubsubError;
export declare function send(peerId: string, message: InvWantWireMessage): InvWantAction;
export declare function routeHasProvider(route: UpstreamRoute, peerId: string): boolean;
//# sourceMappingURL=mesh-state.d.ts.map