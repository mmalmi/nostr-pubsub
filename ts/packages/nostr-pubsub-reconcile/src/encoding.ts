// Negentropy v1; derived from Doug Hoyte's MIT-licensed reference protocol.
export const INFINITY = 0xffffffffffffffffn;
export interface Bound { timestamp: bigint; id: Uint8Array }
export function compareIds(a: Uint8Array, b: Uint8Array): number {
  for (let i = 0; i < 32; i++) {
    const difference = (a[i] ?? 0) - (b[i] ?? 0);
    if (difference) return difference;
  }
  return 0;
}
export function compare(a: Bound, b: Bound): number {
  return a.timestamp < b.timestamp ? -1 : a.timestamp > b.timestamp ? 1 : compareIds(a.id, b.id);
}
export function key(id: Uint8Array): string {
  return Array.from(id, n => n.toString(16).padStart(2, '0')).join('');
}
export class Reader {
  offset = 0;
  timestamp = 0n;
  constructor(readonly bytes: Uint8Array) {}
  get remaining(): number { return this.bytes.length - this.offset; }
  take(n: number): Uint8Array {
    if (!Number.isSafeInteger(n) || n < 0 || n > this.remaining) throw new Error('truncated frame');
    const result = this.bytes.subarray(this.offset, this.offset + n);
    this.offset += n;
    return result;
  }
  varint(): bigint {
    let value = 0n;
    for (let i = 0; i < 10; i++) {
      const byte = this.take(1)[0];
      value = value * 128n + BigInt(byte & 127);
      if (value > INFINITY) throw new Error('varint overflow');
      if (!(byte & 128)) return value;
    }
    throw new Error('varint too long');
  }
  bound(): Bound {
    const delta = this.varint();
    this.timestamp = delta === 0n || this.timestamp === INFINITY ? INFINITY : this.timestamp + delta - 1n;
    if (this.timestamp > INFINITY) throw new Error('timestamp overflow');
    const length = this.varint();
    if (length > 32n) throw new Error('invalid bound');
    return { timestamp: this.timestamp, id: this.take(Number(length)) };
  }
}
export class Writer {
  bytes: number[] = [];
  timestamp = 0n;
  get length(): number { return this.bytes.length; }
  append(bytes: Uint8Array | number[]): void { for (const byte of bytes) this.bytes.push(byte); }
  varint(value: bigint): void {
    if (value < 0n || value > INFINITY) throw new Error('invalid varint');
    const bytes = [Number(value & 127n)];
    while ((value >>= 7n) > 0n) bytes.push(Number(value & 127n) | 128);
    this.append(bytes.reverse());
  }
  bound(bound: Bound): void {
    this.varint(bound.timestamp === INFINITY ? 0n : bound.timestamp - this.timestamp + 1n);
    this.timestamp = bound.timestamp;
    this.varint(BigInt(bound.id.length));
    this.append(bound.id);
  }
  finish(): Uint8Array { return Uint8Array.from(this.bytes); }
}
