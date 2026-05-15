/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.encoder;

import org.apache.lucene.store.DataInput;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;

import java.io.IOException;

/**
 * Identity {@link BlockEncoding}: writes input bytes through verbatim and returns the input
 * stream directly on read, so reads stay off-heap when the underlying {@code IndexInput} is
 * mmap'd. The encoded length always equals the input length.
 *
 * <p>This is the v0 default of {@link ColumNARDocValuesFormat}. Production deployments swap in a
 * compressing encoding (LZ4 / Zstd, future) through the same seam; keeping the identity
 * implementation around lets the layout be measured in isolation.
 */
public final class IdentityBlockEncoding implements BlockEncoding {

    public static final String NAME = "Identity";
    public static final IdentityBlockEncoding INSTANCE = new IdentityBlockEncoding();

    private static final Encoder ENCODER = (src, srcOffset, srcLen, out) -> {
        out.writeBytes(src, srcOffset, srcLen);
        return srcLen;
    };

    public IdentityBlockEncoding() {}

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Encoder newEncoder() {
        // Stateless — the same lambda is safe to share across every consumer.
        return ENCODER;
    }

    @Override
    public DataInput decode(int formatVersion, DataInput in, int encodedLen, byte[] scratch, int decodedLen) throws IOException {
        if (encodedLen != decodedLen) {
            throw new IOException("IdentityBlockEncoding expects encodedLen == decodedLen, got " + encodedLen + " / " + decodedLen);
        }
        // Zero-copy pass-through: hand back the input so the encoder reads straight from
        // mmap'd memory (when the underlying IndexInput is FSDirectory + MMapDirectory).
        return in;
    }
}
