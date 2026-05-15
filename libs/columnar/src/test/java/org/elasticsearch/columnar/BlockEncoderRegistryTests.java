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
import org.elasticsearch.columnar.encoder.BlockEncodingRegistry;
import org.elasticsearch.columnar.encoder.IdentityBlockEncoding;
import org.elasticsearch.columnar.encoder.NumericBlockEncoderRegistry;
import org.elasticsearch.columnar.encoder.RawBlockEncoder;
import org.elasticsearch.test.ESTestCase;

public class BlockEncoderRegistryTests extends ESTestCase {

    public void testBuiltInEncodersRegistered() {
        // NamedSPILoader returns the ServiceLoader-instantiated copy — not the same identity
        // as the typed INSTANCE constant, but the same class and the same behaviour.
        assertEquals(RawBlockEncoder.class, NumericBlockEncoderRegistry.forName(RawBlockEncoder.NAME).getClass());
        assertEquals(BitPackBlockEncoder.class, NumericBlockEncoderRegistry.forName(BitPackBlockEncoder.NAME).getClass());
    }

    public void testBuiltInEncodingsRegistered() {
        assertEquals(IdentityBlockEncoding.class, BlockEncodingRegistry.forName(IdentityBlockEncoding.NAME).getClass());
    }

    public void testUnknownNamesThrow() {
        expectThrows(IllegalArgumentException.class, () -> NumericBlockEncoderRegistry.forName("DefinitelyNotARealEncoder"));
        expectThrows(IllegalArgumentException.class, () -> BlockEncodingRegistry.forName("DefinitelyNotARealEncoding"));
    }

    public void testAvailableNamesContainsBuiltIns() {
        assertTrue(NumericBlockEncoderRegistry.availableNames().contains(RawBlockEncoder.NAME));
        assertTrue(NumericBlockEncoderRegistry.availableNames().contains(BitPackBlockEncoder.NAME));
        assertTrue(BlockEncodingRegistry.availableNames().contains(IdentityBlockEncoding.NAME));
    }
}
