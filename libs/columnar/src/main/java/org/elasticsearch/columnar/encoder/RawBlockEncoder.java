/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.encoder;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataInput;

import java.io.IOException;

/**
 * Default {@link NumericBlockEncoder} baseline: writes each long verbatim using Lucene's standard
 * {@link org.apache.lucene.store.DataOutput#writeLong} byte order. The encoded output for
 * {@code N} values is exactly {@code 8 * N} bytes long, which makes this the cheapest possible
 * encoder and a useful reference for benchmarking alternative encoders.
 *
 * <p>Reads stay zero-copy: when paired with {@link IdentityBlockEncoding} the {@code DataInput}
 * passed to {@link #decode} is the underlying {@code IndexInput} itself, so {@code readLong}
 * pulls straight from the mmap'd file region.
 */
public final class RawBlockEncoder implements NumericBlockEncoder {

    public static final String NAME = "Raw";
    public static final RawBlockEncoder INSTANCE = new RawBlockEncoder();

    public RawBlockEncoder() {}

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int maxEncodedSize(int valuesLen) {
        return Math.multiplyExact(valuesLen, Long.BYTES);
    }

    @Override
    public int encode(long[] values, int valuesOffset, int valuesLen, byte[] dest, int destOffset) {
        final ByteArrayDataOutput out = new ByteArrayDataOutput(dest, destOffset, dest.length - destOffset);
        for (int i = 0; i < valuesLen; i++) {
            out.writeLong(values[valuesOffset + i]);
        }
        return out.getPosition() - destOffset;
    }

    @Override
    public void decode(int formatVersion, DataInput in, long[] dest, int destOffset, int valuesLen, long[] scratch) throws IOException {
        for (int i = 0; i < valuesLen; i++) {
            dest[destOffset + i] = in.readLong();
        }
    }
}
