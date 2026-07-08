/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.OffHeapFSTStore;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.elasticsearch.index.engine.PartitionedManifest.Unit;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * The compacted form of {@link PartitionedManifest}: an FST keyed by {@code partition + SEP + unitName} with the
 * unit weight as output, answering partition-scoped queries in O(#partition units) via a prefix scan and holding
 * millions of units in tens of MB rather than the ~GB the on-heap maps need. {@link #build} makes an on-heap FST
 * to {@link #save}; {@link #openOffHeap} mmaps the {@code .fst} data via {@link OffHeapFSTStore} so the bytes live
 * in the OS page cache (or, in stateless, the shared blob cache), <b>not the Java heap</b> — only the FST metadata
 * is retained on-heap.
 * <p>
 * The separator byte sorts before any name byte, so a partition's units group contiguously and a longer partition
 * name never falls inside a shorter one's prefix range; unit and partition names must not contain it.
 */
public final class FstPartitionCatalog implements Closeable {

    private static final char SEP = ' ';
    private static final String META_CODEC = "FstPartitionCatalogMeta";
    private static final int VERSION = 1;

    private final FST<Long> fst;
    private final int unitCount;
    private final IndexInput dataInput; // non-null only when opened off-heap; owns the mmap handle

    private FstPartitionCatalog(FST<Long> fst, int unitCount, IndexInput dataInput) {
        this.fst = fst;
        this.unitCount = unitCount;
        this.dataInput = dataInput;
    }

    /** Builds an on-heap catalog. Unit names must be unique. */
    public static FstPartitionCatalog build(Collection<Unit> units) throws IOException {
        final List<Unit> sorted = new ArrayList<>(units);
        sorted.sort(Comparator.comparing(u -> u.partition() + SEP + u.name()));

        final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
        final FSTCompiler<Long> compiler = new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE1, outputs).build();
        final IntsRefBuilder scratch = new IntsRefBuilder();
        for (Unit u : sorted) {
            final BytesRef key = new BytesRef(u.partition() + SEP + u.name());
            compiler.add(Util.toIntsRef(key, scratch), u.weight());
        }
        final FST<Long> fst = FST.fromFSTReader(compiler.compile(), compiler.getFSTReader());
        return new FstPartitionCatalog(fst, sorted.size(), null);
    }

    /** Persists the FST as two blobs: {@code <name>.fst} (data) and {@code <name>.fstmeta} (metadata + count). */
    public void save(Directory dir, String name) throws IOException {
        try (
            IndexOutput dataOut = dir.createOutput(name + ".fst", IOContext.DEFAULT);
            IndexOutput metaOut = dir.createOutput(name + ".fstmeta", IOContext.DEFAULT)
        ) {
            CodecUtil.writeHeader(metaOut, META_CODEC, VERSION);
            metaOut.writeVInt(unitCount);
            fst.save(metaOut, dataOut); // FST metadata -> metaOut, FST data -> dataOut
            CodecUtil.writeFooter(metaOut);
            CodecUtil.writeFooter(dataOut);
        }
    }

    /**
     * Opens the catalog off-heap: the {@code .fst} data file is mmap'd and the FST reads directly from it. The
     * returned catalog owns the mmap handle and must be {@link #close() closed}.
     */
    public static FstPartitionCatalog openOffHeap(Directory dir, String name) throws IOException {
        final int unitCount;
        final FST.FSTMetadata<Long> metadata;
        try (ChecksumIndexInput metaIn = dir.openChecksumInput(name + ".fstmeta")) {
            CodecUtil.checkHeader(metaIn, META_CODEC, VERSION, VERSION);
            unitCount = metaIn.readVInt();
            metadata = FST.readMetadata(metaIn, PositiveIntOutputs.getSingleton());
            CodecUtil.checkFooter(metaIn);
        }
        final IndexInput dataInput = dir.openInput(name + ".fst", IOContext.DEFAULT);
        boolean success = false;
        try {
            final OffHeapFSTStore store = new OffHeapFSTStore(dataInput, 0, metadata);
            final FstPartitionCatalog catalog = new FstPartitionCatalog(FST.fromFSTReader(metadata, store), unitCount, dataInput);
            success = true;
            return catalog;
        } finally {
            if (success == false) {
                dataInput.close();
            }
        }
    }

    /** Total number of units in the catalog. */
    public int unitCount() {
        return unitCount;
    }

    /** Live units of one partition, via an FST prefix scan. O(#partition units). */
    public List<Unit> units(String partition) throws IOException {
        final BytesRef prefix = new BytesRef(partition + SEP);
        final List<Unit> result = new ArrayList<>();
        final BytesRefFSTEnum<Long> en = new BytesRefFSTEnum<>(fst);
        BytesRefFSTEnum.InputOutput<Long> io = en.seekCeil(prefix);
        final int nameStart = partition.length() + 1; // partition bytes + the 1-byte separator
        while (io != null && StringHelper.startsWith(io.input, prefix)) {
            final String name = io.input.utf8ToString().substring(nameStart);
            result.add(new Unit(name, partition, io.output));
            io = en.next();
        }
        return result;
    }

    /** Number of units in one partition. */
    public int unitCount(String partition) throws IOException {
        final BytesRef prefix = new BytesRef(partition + SEP);
        int count = 0;
        final BytesRefFSTEnum<Long> en = new BytesRefFSTEnum<>(fst);
        BytesRefFSTEnum.InputOutput<Long> io = en.seekCeil(prefix);
        while (io != null && StringHelper.startsWith(io.input, prefix)) {
            count++;
            io = en.next();
        }
        return count;
    }

    /** Enumerates every unit (used to rebuild a new base at compaction). O(#total). */
    public List<Unit> allUnits() throws IOException {
        final List<Unit> result = new ArrayList<>(unitCount);
        final BytesRefFSTEnum<Long> en = new BytesRefFSTEnum<>(fst);
        BytesRefFSTEnum.InputOutput<Long> io = en.next();
        while (io != null) {
            final String key = io.input.utf8ToString();
            final int sep = key.indexOf(SEP);
            result.add(new Unit(key.substring(sep + 1), key.substring(0, sep), io.output));
            io = en.next();
        }
        return result;
    }

    /** Approximate retained heap: for an off-heap catalog this is the FST metadata only (bytes), not the data. */
    public long ramBytesUsed() {
        return fst.ramBytesUsed();
    }

    @Override
    public void close() throws IOException {
        if (dataInput != null) {
            dataInput.close();
        }
    }
}
