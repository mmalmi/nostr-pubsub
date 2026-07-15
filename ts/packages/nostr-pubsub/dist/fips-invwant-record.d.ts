export declare const INV_WANT_RECORD_PREFIX_BYTES = 4;
export declare function encodeInvWantRecord(payload: Uint8Array, maxRecordBytes: number): Uint8Array;
export declare class InvWantRecordDecoder {
    private readonly maxRecordBytes;
    private buffer;
    constructor(maxRecordBytes: number);
    push(bytes: Uint8Array, maxRecords: number): Uint8Array[];
    get length(): number;
    get remainingCapacity(): number;
    get hasCompleteRecord(): boolean;
}
//# sourceMappingURL=fips-invwant-record.d.ts.map