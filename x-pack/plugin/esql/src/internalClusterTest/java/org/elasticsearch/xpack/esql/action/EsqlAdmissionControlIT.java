/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.resource.ResourcePoolStats;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchService;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies that ES|QL data-node execution consumes the node-local shard-search admission budget owned by
 * {@link SearchService} (the same budget classic {@code _search} uses) and fully releases it. The reject/queue/timeout
 * semantics of the underlying pool are unit-tested in {@code ResourcePoolTests}; here we prove the wiring and the
 * no-leak guarantee against a real cluster.
 */
public class EsqlAdmissionControlIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SearchService.SEARCH_ADMISSION_CONTROL_SLOTS_PER_THREAD.getKey(), 4)
            .put(SearchService.SEARCH_ADMISSION_CONTROL_ACQUIRE_TIMEOUT.getKey(), TimeValue.timeValueSeconds(30))
            .build();
    }

    public void testDataNodeWorkGoesThroughAdmissionAndReleases() {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 3)));
        for (int i = 0; i < 50; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("v", i).get();
        }
        client().admin().indices().prepareRefresh("test").get();

        long acquiredBefore = sumAcquired();
        try (EsqlQueryResponse resp = run("FROM test | STATS s = sum(v)")) {
            assertThat(resp, notNullValue());
        }

        // The data-node shard work for the query was admitted through the shared pool...
        assertThat(sumAcquired(), greaterThan(acquiredBefore));
        // ...and every node released what it acquired, leaving no slots held.
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            ResourcePoolStats stats = service.searchAdmissionStats();
            assertThat(stats, notNullValue());
            assertEquals("admission pool leaked slots on a node", 0, stats.currentUsedSlots());
            assertEquals("acquired and released counts diverge on a node", stats.totalAcquired(), stats.totalReleased());
        }
    }

    private long sumAcquired() {
        long total = 0;
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            total += service.searchAdmissionStats().totalAcquired();
        }
        return total;
    }
}
