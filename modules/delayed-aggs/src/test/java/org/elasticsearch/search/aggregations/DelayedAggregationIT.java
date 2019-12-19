/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class DelayedAggregationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(DelayedAggregationPlugin.class);
    }

    public void testSimple() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("index"));
        long start = System.currentTimeMillis();
        SearchResponse response = client().prepareSearch("index")
            .addAggregation(new DelayedAggregationBuilder("delay", "index", TimeValue.timeValueSeconds(2)))
            .get();
        logger.info("Delay1= " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        response = client().prepareSearch("index")
            .addAggregation(new DelayedAggregationBuilder("delay", "ind*", TimeValue.timeValueSeconds(2)))
            .get();
        logger.info("Delay2= " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        response = client().prepareSearch("index")
            .addAggregation(new DelayedAggregationBuilder("delay", "other", TimeValue.timeValueSeconds(2)))
            .get();
        logger.info("Delay3= " + (System.currentTimeMillis() - start));
    }
}
