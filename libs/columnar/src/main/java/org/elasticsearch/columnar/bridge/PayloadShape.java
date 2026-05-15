/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.bridge;

import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Marker byte that every bridge-packed binary doc-values payload carries as its first
 * byte. Lets the bridge readers prove a payload was produced by one of the official
 * packers ({@link PackedLongBinaryPacker} or {@link PackedBytesBinaryPacker}) and fail
 * cleanly when the convention is bypassed.
 *
 * <p><b>Convention contract.</b> The columnar codec is binary-only at the Lucene level —
 * it accepts whatever bytes the mapper puts in a {@link org.apache.lucene.document.BinaryDocValuesField}.
 * Callers MUST go through one of the bridge's typed wrappers
 * ({@link ColumNARLongField}, {@link ColumNARIntField}, {@link ColumNARFloatField},
 * {@link ColumNARDoubleField}, {@link ColumNARKeywordField}) or call the packers directly;
 * those paths prepend the appropriate {@link PayloadShape} byte automatically. A raw
 * {@code BinaryDocValuesField} write that skips the packer ends up with bytes that don't
 * carry a known shape byte; the bridge reader fails fast with {@link #expect} when it
 * tries to decode such a payload.
 *
 * <p>One byte of overhead per doc is the price for a self-describing payload that the
 * codec can stay type-agnostic while still letting consumers reject malformed input
 * immediately.
 */
public enum PayloadShape {

    /** Packed by {@link PackedLongBinaryPacker} — header is {@code [count][long*]}. */
    LONG((byte) 'L'),

    /** Packed by {@link PackedBytesBinaryPacker} — header is {@code [count][len][bytes]*}. */
    BYTES((byte) 'B');

    private final byte marker;

    PayloadShape(byte marker) {
        this.marker = marker;
    }

    /** The 1-byte marker that prefixes every payload of this shape. */
    public byte marker() {
        return marker;
    }

    /**
     * Verify the first byte of {@code payload} matches {@code expected}. Throws a clear
     * {@link IOException} when the convention was bypassed — typically because a caller
     * wrote a raw {@code BinaryDocValuesField} bypassing the bridge packers.
     */
    public static void expect(PayloadShape expected, BytesRef payload) throws IOException {
        if (payload.length < 1) {
            throw new IOException(
                "ColumnarDocValues bridge payload is empty — every binary doc-values value must be "
                    + "packed via "
                    + PackedLongBinaryPacker.class.getSimpleName()
                    + " / "
                    + PackedBytesBinaryPacker.class.getSimpleName()
                    + " (typically through one of the "
                    + "Columnar*Field wrappers)."
            );
        }
        final byte got = payload.bytes[payload.offset];
        if (got != expected.marker) {
            throw new IOException(
                "ColumnarDocValues bridge payload header mismatch: expected "
                    + expected
                    + " (marker '"
                    + (char) (expected.marker & 0xFF)
                    + "', 0x"
                    + Integer.toHexString(expected.marker & 0xFF)
                    + ") but got 0x"
                    + Integer.toHexString(got & 0xFF)
                    + ". Every binary doc-values value must be packed via the bridge packers — "
                    + "do NOT write a raw BinaryDocValuesField directly. Use the Columnar*Field wrappers."
            );
        }
    }
}
