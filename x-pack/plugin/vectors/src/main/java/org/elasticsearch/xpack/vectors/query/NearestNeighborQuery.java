package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.document.FloatPointNearestNeighbor;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;

public class NearestNeighborQuery extends Query {
    private final String field;
    private final int topN;
    private final float[] vector;

    public NearestNeighborQuery(String field, int topN, float[] vector) {
        this.field = field;
        this.topN = topN;
        this.vector = vector;
    }

    private static boolean isDocInSegment(LeafReaderContext context, int doc) {
        return doc >= context.docBase && doc <= context.docBase + context.reader().numDocs();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        final TopFieldDocs topDocs = FloatPointNearestNeighbor.nearest(searcher, field, topN, vector);
        Arrays.sort(topDocs.scoreDocs, Comparator.comparing(s -> s.doc));
        ScoreDoc[] docs = topDocs.scoreDocs;
        int[] pos = new int[1];
        pos[0] = -1;
        return new Weight(this) {
            @Override
            public void extractTerms(Set<Term> terms) {}

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                // TODO
                return null;
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                return new Scorer(this) {
                    int doc = -1;
                    @Override
                    public DocIdSetIterator iterator() {
                        return new DocIdSetIterator() {
                            @Override
                            public int docID() {
                                return doc;
                            }

                            @Override
                            public int nextDoc() throws IOException {
                                if (++pos[0] >= docs.length || isDocInSegment(context, docs[pos[0]].doc) == false) {
                                    pos[0]--;
                                    return doc = NO_MORE_DOCS;
                                }
                                return doc = docs[pos[0]].doc - context.docBase;
                            }

                            @Override
                            public int advance(int target) throws IOException {
                                while (++pos[0] < docs.length && isDocInSegment(context, docs[pos[0]].doc)) {
                                    if (docs[pos[0]].doc - context.docBase >= target) {
                                        return doc = docs[pos[0]].doc;
                                    }
                                }
                                pos[0] --;
                                return doc = NO_MORE_DOCS;
                            }

                            @Override
                            public long cost() {
                                return topN;
                            }
                        };
                    }

                    @Override
                    public float getMaxScore(int upTo) throws IOException {
                        return Float.POSITIVE_INFINITY;
                    }

                    @Override
                    public float score() throws IOException {
                        return 1f / (1f + (float) (((FieldDoc) docs[pos[0]]).fields[0]));
                    }

                    @Override
                    public int docID() {
                        return doc;
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }

    @Override
    public String toString(String field) {
        // TODO
        return field;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NearestNeighborQuery that = (NearestNeighborQuery) o;
        return topN == that.topN &&
            field.equals(that.field) &&
            Arrays.equals(vector, that.vector);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(field, topN);
        result = 31 * result + Arrays.hashCode(vector);
        return result;
    }
}
