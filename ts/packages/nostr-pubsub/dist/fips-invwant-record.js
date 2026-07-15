import { PubsubError } from './types.js';
export const INV_WANT_RECORD_PREFIX_BYTES = 4;
export function encodeInvWantRecord(payload, maxRecordBytes) {
    if (payload.byteLength > maxRecordBytes) {
        throw validation(`record is ${payload.byteLength} bytes, maximum is ${maxRecordBytes}`);
    }
    const record = new Uint8Array(INV_WANT_RECORD_PREFIX_BYTES + payload.byteLength);
    new DataView(record.buffer).setUint32(0, payload.byteLength, false);
    record.set(payload, INV_WANT_RECORD_PREFIX_BYTES);
    return record;
}
export class InvWantRecordDecoder {
    maxRecordBytes;
    buffer = new Uint8Array();
    constructor(maxRecordBytes) {
        this.maxRecordBytes = maxRecordBytes;
    }
    push(bytes, maxRecords) {
        const attempted = this.buffer.byteLength + bytes.byteLength;
        const capacity = this.maxRecordBytes + INV_WANT_RECORD_PREFIX_BYTES;
        if (!Number.isSafeInteger(attempted) || attempted > capacity) {
            this.buffer = new Uint8Array();
            throw validation(`record input is ${attempted} bytes, maximum buffered input is ${capacity}`);
        }
        if (bytes.byteLength > 0) {
            const combined = new Uint8Array(attempted);
            combined.set(this.buffer);
            combined.set(bytes, this.buffer.byteLength);
            this.buffer = combined;
        }
        const records = [];
        let consumed = 0;
        while (records.length < maxRecords &&
            this.buffer.byteLength - consumed >= INV_WANT_RECORD_PREFIX_BYTES) {
            const declared = new DataView(this.buffer.buffer, this.buffer.byteOffset + consumed, INV_WANT_RECORD_PREFIX_BYTES).getUint32(0, false);
            if (declared > this.maxRecordBytes) {
                this.buffer = new Uint8Array();
                throw validation(`record declares ${declared} bytes, maximum is ${this.maxRecordBytes}`);
            }
            const recordBytes = INV_WANT_RECORD_PREFIX_BYTES + declared;
            if (this.buffer.byteLength - consumed < recordBytes)
                break;
            const start = consumed + INV_WANT_RECORD_PREFIX_BYTES;
            records.push(this.buffer.slice(start, consumed + recordBytes));
            consumed += recordBytes;
        }
        if (consumed === this.buffer.byteLength)
            this.buffer = new Uint8Array();
        else if (consumed > 0)
            this.buffer = this.buffer.slice(consumed);
        return records;
    }
    get length() {
        return this.buffer.byteLength;
    }
    get remainingCapacity() {
        return Math.max(0, this.maxRecordBytes + INV_WANT_RECORD_PREFIX_BYTES - this.buffer.byteLength);
    }
    get hasCompleteRecord() {
        if (this.buffer.byteLength < INV_WANT_RECORD_PREFIX_BYTES)
            return false;
        const declared = new DataView(this.buffer.buffer, this.buffer.byteOffset, INV_WANT_RECORD_PREFIX_BYTES).getUint32(0, false);
        return declared <= this.maxRecordBytes &&
            this.buffer.byteLength >= INV_WANT_RECORD_PREFIX_BYTES + declared;
    }
}
function validation(message) {
    return PubsubError.validation(message);
}
//# sourceMappingURL=fips-invwant-record.js.map