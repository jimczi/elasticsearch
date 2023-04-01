/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.dfs;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DfsHybridResults implements Writeable {
    private final List<ScoreDoc[]> scoreShardDocs;

    public DfsHybridResults(List<ScoreDoc[]> scoreShardDocs) {
        this.scoreShardDocs = scoreShardDocs;
    }

    public DfsHybridResults(StreamInput in) throws IOException {
        int len = in.readVInt();
        this.scoreShardDocs = new ArrayList<>(len);
        for (int i = 0; i < len; i++){
            scoreShardDocs.add(in.readArray(Lucene::readScoreDoc, ScoreDoc[]::new));
        }
    }

    public List<ScoreDoc[]> getScoreShardDocs() {
        return scoreShardDocs;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(scoreShardDocs.size());
        for (ScoreDoc[] docs : scoreShardDocs) {
            out.writeArray(Lucene::writeScoreDoc, docs);
        }
    }
}
