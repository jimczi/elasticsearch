/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.action.ActionListener;

import java.util.List;
import java.util.Map;

public abstract class ModelRegistry {
    public record ModelConfigMap(Map<String, Object> config, Map<String, Object> secrets) {}

    /**
     * Semi parsed model where model id, task type and service
     * are known but the settings are not parsed.
     */
    public record UnparsedModel(
        String modelId,
        TaskType taskType,
        String service,
        Map<String, Object> settings,
        Map<String, Object> secrets
    ) {}

    public abstract void getModelWithSecrets(String modelId, ActionListener<UnparsedModel> listener);

    public abstract void getModel(String modelId, ActionListener<UnparsedModel> listener);

    public abstract void getModelsByTaskType(TaskType taskType, ActionListener<List<UnparsedModel>> listener);

    public abstract void storeModel(Model model, ActionListener<Boolean> listener);

    public abstract void deleteModel(String modelId, ActionListener<Boolean> listener);

    public abstract void getAllModels(ActionListener<List<UnparsedModel>> listener);
}
