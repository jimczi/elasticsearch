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
import org.elasticsearch.columnar.encoder.BlockEncodingRegistry;
import org.elasticsearch.columnar.encoder.Lz4BlockEncoding;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class Lz4BlockEncodingTests extends ESTestCase {

    public void testIdReservedAtOne() {
        assertEquals("Lz4", Lz4BlockEncoding.INSTANCE.getName());
        assertEquals("Lz4", Lz4BlockEncoding.HIGH.getName());
        assertEquals("Lz4", new Lz4BlockEncoding().getName());
    }

    public void testRoundTripFastMode() throws IOException {
        assertRoundTrip(Lz4BlockEncoding.INSTANCE, randomPayload(1024));
        assertRoundTrip(Lz4BlockEncoding.INSTANCE, randomPayload(8192));
        assertRoundTrip(Lz4BlockEncoding.INSTANCE, repeatingPayload(8192));
    }

    public void testRoundTripHighMode() throws IOException {
        assertRoundTrip(Lz4BlockEncoding.HIGH, randomPayload(1024));
        assertRoundTrip(Lz4BlockEncoding.HIGH, randomPayload(8192));
        assertRoundTrip(Lz4BlockEncoding.HIGH, repeatingPayload(8192));
    }

    /**
     * The key BWC invariant: the level is encoder-only state and must NOT appear on disk.
     * Bytes written with HIGH must be decodable by a reader instantiated as FAST (the SPI
     * default), and vice versa.
     */
    public void testDecoderIgnoresWriterLevel() throws IOException {
        final byte[] payload = randomPayload(4096);
        // Write with HIGH...
        final byte[] disk = encode(Lz4BlockEncoding.HIGH, payload);
        // ...and read with the SPI default (FAST). Decoder must produce identical bytes.
        final byte[] decoded = decode(Lz4BlockEncoding.INSTANCE, disk, payload.length);
        assertArrayEquals(payload, decoded);
    }

    public void testHighReachesSmallerSizeOnCompressibleInput() throws IOException {
        final byte[] payload = repeatingPayload(8192);
        final byte[] fastDisk = encode(Lz4BlockEncoding.INSTANCE, payload);
        final byte[] highDisk = encode(Lz4BlockEncoding.HIGH, payload);
        // Both compress well; HIGH is at least as small as FAST on this input.
        assertTrue(
            "HIGH should compress no worse than FAST on a repeating payload (fast=" + fastDisk.length + ", high=" + highDisk.length + ")",
            highDisk.length <= fastDisk.length
        );
    }

    public void testEmptyAndTinyPayloads() throws IOException {
        assertRoundTrip(Lz4BlockEncoding.INSTANCE, new byte[0]);
        assertRoundTrip(Lz4BlockEncoding.INSTANCE, new byte[] { 0 });
        assertRoundTrip(Lz4BlockEncoding.INSTANCE, new byte[] { 1, 2, 3, 4, 5 });
    }

    public void testRegistryReturnsLz4() {
        final BlockEncoding registered = BlockEncodingRegistry.forName(Lz4BlockEncoding.NAME);
        assertNotNull("LZ4 must be registered by id", registered);
        assertTrue(registered instanceof Lz4BlockEncoding);
    }

    private void assertRoundTrip(Lz4BlockEncoding encoding, byte[] payload) throws IOException {
        final byte[] disk = encode(encoding, payload);
        final byte[] decoded = decode(encoding, disk, payload.length);
        assertArrayEquals(payload, decoded);
    }

    private byte[] encode(Lz4BlockEncoding encoding, byte[] payload) throws IOException {
        final BlockEncoding.Encoder writer = encoding.newEncoder();
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        final int written = writer.encode(payload, 0, payload.length, out);
        final byte[] bytes = out.toArrayCopy();
        assertEquals("encode return value must match bytes written", bytes.length, written);
        return bytes;
    }

    private byte[] decode(Lz4BlockEncoding encoding, byte[] disk, int decodedLen) throws IOException {
        final byte[] scratch = new byte[Math.max(1, decodedLen)];
        final DataInput in = new ByteArrayDataInput(disk);
        final DataInput decoded = encoding.decode(ColumNARDocValuesFormat.VERSION_CURRENT, in, disk.length, scratch, decodedLen);
        final byte[] out = new byte[decodedLen];
        decoded.readBytes(out, 0, decodedLen);
        return out;
    }

    private byte[] randomPayload(int size) {
        return randomByteArrayOfLength(size);
    }

    private byte[] repeatingPayload(int size) {
        final byte[] b = new byte[size];
        final byte[] pattern = "elasticsearch".getBytes();
        for (int i = 0; i < size; i++) {
            b[i] = pattern[i % pattern.length];
        }
        return b;
    }
}
