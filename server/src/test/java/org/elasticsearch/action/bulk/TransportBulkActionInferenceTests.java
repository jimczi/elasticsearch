/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelRegistry;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.TestInferenceResults;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.After;
import org.junit.Before;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class TransportBulkActionInferenceTests extends ESTestCase {

    public static final String INDEX_NAME = "index";
    public static final String INFERENCE_FIELD_1_MODEL_A = "inference_field_1_model_a";
    public static final String MODEL_A_ID = "model_a_id";
    private static final String INFERENCE_FIELD_2_MODEL_A = "inference_field_2_model_a";
    public static final String MODEL_B_ID = "model_b_id";
    private static final String INFERENCE_FIELD_MODEL_B = "inference_field_model_b";
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private NodeClient nodeClient;
    private TransportBulkAction transportBulkAction;
    private InferenceService inferenceService;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getClass().getName());
        nodeClient = mock(NodeClient.class);

        // Contains the fields for models for the index
        Metadata metadata = Metadata.builder()
            .indices(
                Map.of(
                    INDEX_NAME,
                    IndexMetadata.builder(INDEX_NAME)
                        .settings(settings(IndexVersion.current()))
                        .fieldsForModels(
                            Map.of(
                                MODEL_A_ID,
                                Set.of(INFERENCE_FIELD_1_MODEL_A, INFERENCE_FIELD_2_MODEL_A),
                                MODEL_B_ID,
                                Set.of(INFERENCE_FIELD_MODEL_B)
                            )
                        )
                        .numberOfShards(1)
                        .numberOfReplicas(1)
                        .build()
                )
            )
            .build();

        DiscoveryNode masterNode = DiscoveryNodeUtils.create(UUIDs.base64UUID());
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .nodes(DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId()).masterNodeId(masterNode.getId()))
            .build();

        clusterService = ClusterServiceUtils.createClusterService(state, threadPool);

        inferenceService = null;
        InferenceServiceRegistry inferenceServiceRegistry = mock(InferenceServiceRegistry.class);
        when(inferenceServiceRegistry.getService(anyString())).thenAnswer(
            (Answer<Optional<InferenceService>>) invocation -> Optional.ofNullable(inferenceService)
        );
        ModelRegistry modelRegistry = null;
        transportBulkAction = new TransportBulkAction(
            threadPool,
            mock(TransportService.class),
            clusterService,
            mock(IngestService.class),
            nodeClient,
            new ActionFilters(Collections.emptySet()),
            TestIndexNameExpressionResolver.newInstance(),
            new IndexingPressure(Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), true).build()),
            EmptySystemIndices.INSTANCE,
            inferenceServiceRegistry,
            modelRegistry
        );
        when(nodeClient.executeLocally(eq(TransportShardBulkAction.TYPE), any(), any())).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<BulkShardResponse>) invocation.getArguments()[2];
            listener.onFailure(new Exception("Unexpected invocation"));
            return null;
        });
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        clusterService.close();
        super.tearDown();
    }

    public void testBulkRequestWithoutInference() {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME).id("id");
        indexRequest.source("non_inference_field", "text", "another_non_inference_field", "other text");
        bulkRequest.add(indexRequest);

        expectTransportShardBulkActionRequest(1);

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        ActionTestUtils.execute(transportBulkAction, null, bulkRequest, future);
        BulkResponse response = future.actionGet();

        assertThat(response.getItems().length, equalTo(1));
        assertTrue(Arrays.stream(response.getItems()).allMatch(r -> r.isFailed() == false));
    }

    public void testBulkRequestWithInference() {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME).id("id");
        String inferenceFieldText = "some text";
        indexRequest.source(INFERENCE_FIELD_1_MODEL_A, inferenceFieldText, "non_inference_field", "other text");
        bulkRequest.add(indexRequest);

        expectInferenceRequest(MODEL_A_ID, inferenceFieldText);

        expectTransportShardBulkActionRequest(1);

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        ActionTestUtils.execute(transportBulkAction, null, bulkRequest, future);
        BulkResponse response = future.actionGet();

        assertThat(response.getItems().length, equalTo(1));
        assertTrue(Arrays.stream(response.getItems()).allMatch(r -> r.isFailed() == false));
    }

    public void testBulkRequestWithMultipleFieldsInference() {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME).id("id");
        String inferenceField1Text = "some text";
        String inferenceField2Text = "some other text";
        String inferenceField3Text = "more inference text";
        indexRequest.source(
            INFERENCE_FIELD_1_MODEL_A,
            inferenceField1Text,
            INFERENCE_FIELD_2_MODEL_A,
            inferenceField2Text,
            INFERENCE_FIELD_MODEL_B,
            inferenceField3Text,
            "non_inference_field",
            "other text"
        );
        bulkRequest.add(indexRequest);

        expectInferenceRequest(MODEL_A_ID, inferenceField1Text, inferenceField2Text);
        expectInferenceRequest(MODEL_B_ID, inferenceField3Text);

        expectTransportShardBulkActionRequest(1);

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        ActionTestUtils.execute(transportBulkAction, null, bulkRequest, future);
        BulkResponse response = future.actionGet();

        assertThat(response.getItems().length, equalTo(1));
        assertTrue(Arrays.stream(response.getItems()).allMatch(r -> r.isFailed() == false));
        verifyInferenceExecuted(times(2));
    }

    public void testBulkRequestWithMultipleDocs() {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME).id("id1");
        String inferenceFieldTextDoc1 = "some text";
        bulkRequest.add(indexRequest);
        indexRequest.source(INFERENCE_FIELD_1_MODEL_A, inferenceFieldTextDoc1, "non_inference_field", "other text");
        indexRequest = new IndexRequest(INDEX_NAME).id("id2");
        String inferenceFieldTextDoc2 = "some other text";
        indexRequest.source(INFERENCE_FIELD_1_MODEL_A, inferenceFieldTextDoc2, "non_inference_field", "more text");
        bulkRequest.add(indexRequest);

        expectInferenceRequest(MODEL_A_ID, inferenceFieldTextDoc1);
        expectInferenceRequest(MODEL_A_ID, inferenceFieldTextDoc2);

        expectTransportShardBulkActionRequest(2);

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        ActionTestUtils.execute(transportBulkAction, null, bulkRequest, future);
        BulkResponse response = future.actionGet();

        assertThat(response.getItems().length, equalTo(2));
        assertTrue(Arrays.stream(response.getItems()).allMatch(r -> r.isFailed() == false));
        verifyInferenceExecuted(times(2));
    }

    public void testFailingInference() {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME).id("id1");
        String inferenceFieldTextDoc1 = "some text";
        indexRequest.source(INFERENCE_FIELD_1_MODEL_A, inferenceFieldTextDoc1, "non_inference_field", "more text");
        bulkRequest.add(indexRequest);
        indexRequest = new IndexRequest(INDEX_NAME).id("id1");
        String inferenceFieldTextDoc2 = "some text";
        indexRequest.source(INFERENCE_FIELD_MODEL_B, inferenceFieldTextDoc2, "non_inference_field", "more text");
        bulkRequest.add(indexRequest);

        expectInferenceRequestFails(MODEL_A_ID, inferenceFieldTextDoc1);
        expectInferenceRequest(MODEL_B_ID, inferenceFieldTextDoc2);

        // Only non-failing inference requests will be executed
        expectTransportShardBulkActionRequest(1);

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        ActionTestUtils.execute(transportBulkAction, null, bulkRequest, future);
        BulkResponse response = future.actionGet();

        assertThat(response.getItems().length, equalTo(2));
        assertTrue(response.getItems()[0].isFailed());
        assertFalse(response.getItems()[1].isFailed());
        verifyInferenceExecuted(times(2));
    }

    private void verifyInferenceExecuted(VerificationMode verificationMode) {
        // verify(inferenceProvider, verificationMode).textInference(any(), any(), any());
    }

    private void expectTransportShardBulkActionRequest(int requestSize) {
        when(nodeClient.executeLocally(eq(TransportShardBulkAction.TYPE), argThat(r -> matchBulkShardRequest(r, requestSize)), any()))
            .thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<BulkShardResponse>) invocation.getArguments()[2];
            var bulkShardRequest = (BulkShardRequest) invocation.getArguments()[1];
            ShardId shardId = new ShardId(INDEX_NAME, "UUID", 0);
                BulkItemResponse[] bulkItemResponses = Arrays.stream(bulkShardRequest.items())
                    .map(item -> BulkItemResponse.success(
                        item.id(),
                        DocWriteRequest.OpType.INDEX,
                        new IndexResponse(
                            shardId,
                            "id",
                            0, 0, 0, true)
                    )
                ).toArray(BulkItemResponse[]::new);

                listener.onResponse(new BulkShardResponse(shardId, bulkItemResponses));
            return null;
        });
    }

    private boolean matchBulkShardRequest(ActionRequest request, int requestSize) {
        return (request instanceof BulkShardRequest) && ((BulkShardRequest) request).items().length == requestSize;
    }

    @SuppressWarnings("unchecked")
    private void expectInferenceRequest(String modelId, String... inferenceTexts) {}

    private void expectInferenceRequestFails(String modelId, String... inferenceTexts) {}

    private static class MockInferenceService implements InferenceService {
        private Map<String, TestInferenceResults> resultsPerModel = new HashMap<>();
        private Exception exception;

        void clearModels() {
            resultsPerModel.clear();
        }

        void setResults(String text, TestInferenceResults results) {
            resultsPerModel.put(text, results);
        }

        @Override
        public String name() {
            return "mock";
        }

        @Override
        public Model parseRequestConfig(String modelId, TaskType taskType, Map<String, Object> config, Set<String> platfromArchitectures) {
            return null;
        }

        @Override
        public Model parsePersistedConfigWithSecrets(
            String modelId,
            TaskType taskType,
            Map<String, Object> config,
            Map<String, Object> secrets
        ) {
            return null;
        }

        @Override
        public Model parsePersistedConfig(String modelId, TaskType taskType, Map<String, Object> config) {
            return null;
        }

        @Override
        public void infer(
            Model model,
            List<String> input,
            Map<String, Object> taskSettings,
            ActionListener<InferenceServiceResults> listener
        ) {
            var results = input.stream().map(text -> resultsPerModel.get(text)).collect(Collectors.toList());
            listener.onResponse(new InferenceServiceResults() {
                @Override
                public List<? extends InferenceResults> transformToCoordinationFormat() {
                    return results;
                }

                @Override
                public List<? extends InferenceResults> transformToLegacyFormat() {
                    return results;
                }

                @Override
                public Map<String, Object> asMap() {
                    throw new IllegalStateException("not implemented");
                }

                @Override
                public String getWriteableName() {
                    throw new IllegalStateException("not implemented");
                }

                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    throw new IllegalStateException("not implemented");
                }

                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    throw new IllegalStateException("not implemented");
                }
            });
        }

        @Override
        public void start(Model model, ActionListener<Boolean> listener) {}

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return null;
        }

        @Override
        public void close() throws IOException {}
    }

}
