import { compare, Reader, type Bound } from './encoding.js';

/** Validate the complete bounded input before frame truncation or any await can
 * stop range processing. Parsing only the output-producing prefix is unsafe. */
export function validateFrame(bytes: Uint8Array): void {
  const reader = new Reader(bytes);
  if (reader.take(1)[0] !== 0x61) throw new Error('unsupported Negentropy version');
  let previous: Bound = { timestamp: 0n, id: new Uint8Array() };
  while (reader.remaining) {
    const bound = reader.bound();
    if (compare(bound, previous) < 0) throw new Error('unordered ranges');
    previous = bound;
    const mode = reader.varint();
    if (mode === 0n) continue;
    if (mode === 1n) reader.take(16);
    else if (mode === 2n) {
      const count = reader.varint();
      if (count > BigInt(Math.floor(reader.remaining / 32))) throw new Error('truncated ID list');
      reader.take(Number(count) * 32);
    } else throw new Error('invalid reconciliation mode');
  }
}
