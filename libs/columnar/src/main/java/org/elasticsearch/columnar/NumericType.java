/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.elasticsearch.columnar.encoder.BitPackBlockEncoder;
import org.elasticsearch.columnar.encoder.DeltaPackedBlockEncoder;
import org.elasticsearch.columnar.encoder.NumericBlockEncoder;

/**
 * Semantic numeric type a {@link NumericBlockEncoder} is designed for. The format always
 * stores 64-bit slots on disk, but encoders may specialise for a narrower or floating-point
 * bit pattern (e.g. a histogram encoder that only makes sense for IEEE doubles). The id is
 * persisted in per-field metadata so the producer can validate that a column written with
 * one numeric semantic isn't read back as another.
 *
 * <p>Generic encoders that work on raw bit patterns regardless of interpretation (e.g.
 * {@link BitPackBlockEncoder}, {@link DeltaPackedBlockEncoder}) declare {@link #LONG} — the
 * universal type. Type-specialised encoders declare a narrower value.
 */
public enum NumericType {
    /** 64-bit signed integer. The universal type — accepts any 64-bit bit pattern. */
    LONG(0),
    /** 32-bit signed integer sign-extended to long. */
    INT(1),
    /** IEEE 754 binary32 reinterpreted as a 32-bit int via {@link Float#floatToRawIntBits}. */
    FLOAT(2),
    /** IEEE 754 binary64 reinterpreted as a 64-bit long via {@link Double#doubleToRawLongBits}. */
    DOUBLE(3);

    private final int id;

    NumericType(int id) {
        this.id = id;
    }

    /** Stable id persisted in metadata. Once shipped, never reused. */
    public int id() {
        return id;
    }

    /** Lookup by persisted id. Throws {@link IllegalArgumentException} for unknown ids. */
    public static NumericType fromId(int id) {
        for (NumericType t : values()) {
            if (t.id == id) {
                return t;
            }
        }
        throw new IllegalArgumentException("unknown NumericType id: " + id);
    }
}
