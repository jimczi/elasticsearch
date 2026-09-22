/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.store.IOContext;

import java.util.Objects;

/**
 * The field a vectors file holds, for a directory that decides what to do with a file from the
 * mapping of the field it belongs to.
 *
 * <p>A file is only named for the format that wrote it and a suffix, so nothing in the name says
 * which field it holds. The component opening it knows: a reader from the field infos of the
 * segment it reads, a writer from the field it was built for. Files covering more than one field
 * carry no hint, and a directory decides for them without the mapping.
 */
public record VectorFieldHint(String field) implements IOContext.FileOpenHint {

    public VectorFieldHint {
        Objects.requireNonNull(field);
    }

    /**
     * Returns the hint for the single field written under {@code segmentSuffix}, or {@code null}
     * when that suffix covers no field or several.
     */
    public static VectorFieldHint forSuffix(FieldInfos fieldInfos, String segmentSuffix) {
        String only = null;
        for (FieldInfo fi : fieldInfos) {
            if (fi.hasVectorValues() == false) {
                continue;
            }
            String format = fi.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY);
            String suffix = fi.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_SUFFIX_KEY);
            if (format == null || suffix == null) {
                continue;
            }
            if (segmentSuffix.equals(format + "_" + suffix) == false) {
                continue;
            }
            if (only != null) {
                // several fields share these files, so the mapping of any one of them does not
                // describe how the file is read
                return null;
            }
            only = fi.name;
        }
        return only == null ? null : new VectorFieldHint(only);
    }
}
