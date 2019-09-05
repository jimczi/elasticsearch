package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class NearestNeighborQueryBuilder extends AbstractQueryBuilder<NearestNeighborQueryBuilder> {
    public static final String NAME = "nearest_neighbor";
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField SIZE_FIELD = new ParseField("size");
    public static final ParseField VECTOR_FIELD = new ParseField("vector");

    private static ConstructingObjectParser<NearestNeighborQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME, false,
        args -> {
            @SuppressWarnings("unchecked")
            List<Float> vectorList = (List<Float>) args[2];
            float[] vector = new float[vectorList.size()];
            for (int i = 0; i < vector.length; i++) {
                vector[i] = vectorList.get(i);
            }
            NearestNeighborQueryBuilder nnBuilder = new NearestNeighborQueryBuilder((String) args[0], (int) args[1], vector);
            return nnBuilder;
        });

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareInt(constructorArg(), SIZE_FIELD);
        PARSER.declareFloatArray(constructorArg(), VECTOR_FIELD);
    }

    public static NearestNeighborQueryBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private String field;
    private int topN;
    private float[] origVector;


    public NearestNeighborQueryBuilder(String field, int topN, float[] origVector) {
        this.field = field;
        this.topN = topN;
        this.origVector = origVector;
    }

    public NearestNeighborQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.field = in.readString();
        this.topN = in.readVInt();
        this.origVector = in.readFloatArray();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeVInt(topN);
        out.writeFloatArray(origVector);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("field", field);
        builder.field("size", topN);
        builder.field("vector", origVector);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        return new NearestNeighborQuery(field, topN, origVector);
    }

    @Override
    protected boolean doEquals(NearestNeighborQueryBuilder other) {
        return Objects.equals(field, other.field)
            && topN == other.topN
            && Arrays.equals(origVector, other.origVector);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, topN, origVector);
    }
}
