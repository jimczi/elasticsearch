/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.attachment;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.attachment.extract.ExtractTextAction;
import org.elasticsearch.ingest.attachment.extract.RestExtractTextAction;
import org.elasticsearch.ingest.attachment.extract.TransportExtractTextAction;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class IngestAttachmentPlugin extends Plugin implements IngestPlugin, ActionPlugin {

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Collections.singletonMap(AttachmentProcessor.TYPE, new AttachmentProcessor.Factory());
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(ExtractTextAction.INSTANCE, TransportExtractTextAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster) {
        return List.of(
            new RestExtractTextAction()
        );
    }
}
