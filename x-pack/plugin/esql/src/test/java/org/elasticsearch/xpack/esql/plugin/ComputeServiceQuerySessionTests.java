/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ComputeServiceQuerySessionTests extends ESTestCase {
    public void testQuerySessionDropsChildSessions() {
        assertThat(ComputeService.querySession("abc"), equalTo("abc"));
        assertThat(ComputeService.querySession("abc/1"), equalTo("abc"));
        assertThat(ComputeService.querySession("abc/1/2"), equalTo("abc"));
    }

    public void testQuerySessionDropsInternalSessions() {
        assertThat(ComputeService.querySession("abc[n]"), equalTo("abc"));
        assertThat(ComputeService.querySession("abc/1[n]"), equalTo("abc"));
    }
}
