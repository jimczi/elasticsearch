/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;


import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.util.function.Supplier;

public class ElasticsearchTrackingDirectory extends FilterDirectory {
    private static final ThreadLocal<ThreadLocalCounters> LOCAL_COUNTERS = ThreadLocal.withInitial(ThreadLocalCounters::new);

    protected ElasticsearchTrackingDirectory(Directory in) {
        super(in);
    }

    public static ThreadLocalCounters localCounters() {
        return LOCAL_COUNTERS.get();
    }

    public static ThreadLocalCounters localCountersClone() {
        return LOCAL_COUNTERS.get().clone();
    }

    public static Supplier<ThreadLocalCounters> deltaSupplier() {
        var counters = LOCAL_COUNTERS.get().clone();
        return () -> counters.delta(LOCAL_COUNTERS.get());
    }

    public static void addBytesRead(long bytesRead) {
        localCounters().addBytesRead(bytesRead);
    }

    public static void addCachedBytesRead(long bytesRead) {
        localCounters().addCachedBytesRead(bytesRead);
    }

    public static void addDiskBytesRead(long bytesRead) {
        localCounters().addDiskBytesRead(bytesRead);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return new ProfileIndexInput(super.openInput(name, context));
    }

    public static class ThreadLocalCounters {
        private long bytesRead;
        private long cachedBytesRead;
        private long diskBytesRead;

        private ThreadLocalCounters() {
        }

        private ThreadLocalCounters(long bytesRead, long cachedBytesRead, long diskBytesRead) {
            this.bytesRead = bytesRead;
            this.cachedBytesRead = cachedBytesRead;
            this.diskBytesRead = diskBytesRead;
        }

        public long bytesRead() {
            return bytesRead;
        }

        public void addBytesRead(long add) {
            bytesRead += add;
        }

        public void addCachedBytesRead(long add) {
            bytesRead += add;
        }

        public void addDiskBytesRead(long add) {
            bytesRead += add;
        }

        public ThreadLocalCounters clone() {
            return new ThreadLocalCounters(bytesRead, cachedBytesRead, diskBytesRead);
        }

        public ThreadLocalCounters delta(ThreadLocalCounters next) {
            return new ThreadLocalCounters(next.bytesRead - bytesRead, next.cachedBytesRead - cachedBytesRead, next.diskBytesRead - diskBytesRead);
        }
    }

    private static class ProfileIndexInput extends FilterIndexInput {
        ProfileIndexInput(IndexInput indexInput) {
            super("profile", indexInput);
        }

        @Override
        public byte readByte() throws IOException {
            addBytesRead(1);
            return super.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            addBytesRead(len);
            super.readBytes(b, offset, len);
        }
    }
}
