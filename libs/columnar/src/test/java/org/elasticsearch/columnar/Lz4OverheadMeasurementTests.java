/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.elasticsearch.columnar.encoder.BlockEncoding;
import org.elasticsearch.columnar.encoder.Lz4BlockEncoding;
import org.elasticsearch.columnar.encoder.NumericBlockEncoder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Locale;
import java.util.Random;

/**
 * Empirical audit of {@link Lz4BlockEncoding} overhead on payloads that are <strong>already
 * highly compressed</strong> by the {@link NumericBlockEncoder} that produced them (e.g. a 2-bit
 * bit-packed dense block). Two questions this informs:
 *
 * <ol>
 *   <li><b>Size penalty.</b> Does LZ4 grow the payload because it can't find redundancy?
 *       If so, by how much?</li>
 *   <li><b>CPU penalty.</b> Is the encode/decode time on incompressible data prohibitive
 *       enough to justify a hint mechanism / runtime "skip encoding when not worth it"
 *       decision?</li>
 * </ol>
 *
 * Throwaway-grade — not asserted, just logs the numbers so a designer can decide whether the
 * complexity of a skip-hint is worth the savings.
 */
public class Lz4OverheadMeasurementTests extends ESTestCase {

    public void testLz4OnHighlyCompressedPayload() throws IOException {
        // Simulate a typical BitPackBlockEncoder output for a 2-bits-per-value dense block
        // of 8192 longs. Bytes = 8 (min header) + 1 (bitsPerValue header) + ceil(8192 * 2 / 8)
        // = 9 + 2048 = 2057 bytes. We use 2048 here as a clean approximation.
        // Bit-packed bytes look pseudorandom — high entropy — so LZ4 should not be able to
        // compress them meaningfully.
        final byte[] bitPackedLike = randomLowEntropyOutput(2048, 42);

        // Run the audit at FAST and HIGH modes.
        runAudit("FAST  / bit-pack-like (2 bpv)", Lz4BlockEncoding.INSTANCE, bitPackedLike);
        runAudit("HIGH  / bit-pack-like (2 bpv)", Lz4BlockEncoding.HIGH, bitPackedLike);

        // Sanity comparison: highly compressible data (mostly zeros) should show LZ4 winning
        // substantially — sanity that the bench harness itself isn't broken.
        final byte[] mostlyZeros = new byte[2048];
        runAudit("FAST  / mostly zeros (compressible)", Lz4BlockEncoding.INSTANCE, mostlyZeros);

        // Truly random data — what a worst-case incompressible payload looks like.
        final byte[] uniformRandom = new byte[2048];
        new Random(7).nextBytes(uniformRandom);
        runAudit("FAST  / uniform random (incompressible)", Lz4BlockEncoding.INSTANCE, uniformRandom);
    }

    private void runAudit(String label, Lz4BlockEncoding encoding, byte[] payload) throws IOException {
        final BlockEncoding.Encoder enc = encoding.newEncoder();

        // Warm-up
        for (int i = 0; i < 50; i++) {
            roundTrip(enc, encoding, payload);
        }

        // Measure encode
        final int iters = 1000;
        final long encStart = System.nanoTime();
        long encodedLen = 0;
        for (int i = 0; i < iters; i++) {
            encodedLen = encodeOnly(enc, payload);
        }
        final long encNs = (System.nanoTime() - encStart) / iters;

        // Measure decode
        final byte[] encoded = encodedBytes(enc, payload);
        final byte[] scratch = new byte[payload.length];
        final long decStart = System.nanoTime();
        for (int i = 0; i < iters; i++) {
            final ByteArrayDataInput in = new ByteArrayDataInput(encoded);
            encoding.decode(ColumNARDocValuesFormat.VERSION_CURRENT, in, encoded.length, scratch, payload.length);
        }
        final long decNs = (System.nanoTime() - decStart) / iters;

        final double sizeRatio = (double) encodedLen / (double) payload.length;
        logger.info(
            String.format(
                Locale.ROOT,
                "Lz4OverheadAudit  %-42s  src=%d  encoded=%d  size-ratio=%.2fx  encode=%dns  decode=%dns",
                label,
                payload.length,
                encodedLen,
                sizeRatio,
                encNs,
                decNs
            )
        );
    }

    private static long encodeOnly(BlockEncoding.Encoder enc, byte[] payload) throws IOException {
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        final int n = enc.encode(payload, 0, payload.length, out);
        return n;
    }

    private static byte[] encodedBytes(BlockEncoding.Encoder enc, byte[] payload) throws IOException {
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        enc.encode(payload, 0, payload.length, out);
        return out.toArrayCopy();
    }

    private static void roundTrip(BlockEncoding.Encoder enc, Lz4BlockEncoding encoding, byte[] payload) throws IOException {
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        enc.encode(payload, 0, payload.length, out);
        final byte[] encoded = out.toArrayCopy();
        final DataInput in = new ByteArrayDataInput(encoded);
        final byte[] scratch = new byte[payload.length];
        encoding.decode(ColumNARDocValuesFormat.VERSION_CURRENT, in, encoded.length, scratch, payload.length);
    }

    /**
     * A payload that looks like a typical bit-packed dense block: pseudo-random bytes (high
     * entropy) — LZ4 won't find runs or back-references, so the output should be at best the
     * input size + a small framing overhead.
     */
    private static byte[] randomLowEntropyOutput(int len, int seed) {
        final byte[] b = new byte[len];
        new Random(seed).nextBytes(b);
        return b;
    }
}
