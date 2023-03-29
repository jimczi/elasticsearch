/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.attachment.extract;

import org.elasticsearch.action.ActionType;

public class ExtractTextAction extends ActionType<ExtractTextResponse> {
    public static final ExtractTextAction INSTANCE = new ExtractTextAction();
    public static final String NAME = "cluster:admin/extract_text";

    public ExtractTextAction() {
        super(NAME, in -> new ExtractTextResponse(in));
    }
}
