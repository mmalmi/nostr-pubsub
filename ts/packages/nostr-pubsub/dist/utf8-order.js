const encoder = new TextEncoder();
/** Orders strings by their UTF-8 bytes, as Rust's `str` ordering does. */
export function compareUtf8(left, right) {
    const shared = Math.min(left.length, right.length);
    for (let index = 0; index < shared; index += 1) {
        const leftUnit = left.charCodeAt(index);
        const rightUnit = right.charCodeAt(index);
        if (leftUnit === rightUnit)
            continue;
        // Below the surrogate range a code unit is its code point, and UTF-8 keeps code point order.
        if (leftUnit < 0xd800 && rightUnit < 0xd800)
            return leftUnit - rightUnit;
        return compareBytes(encoder.encode(left), encoder.encode(right));
    }
    return left.length - right.length;
}
function compareBytes(left, right) {
    const shared = Math.min(left.length, right.length);
    for (let index = 0; index < shared; index += 1) {
        if (left[index] !== right[index])
            return left[index] - right[index];
    }
    return left.length - right.length;
}
//# sourceMappingURL=utf8-order.js.map