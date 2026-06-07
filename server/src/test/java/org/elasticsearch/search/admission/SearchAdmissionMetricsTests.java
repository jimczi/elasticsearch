/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.admission;

import org.elasticsearch.common.resource.Reservation;
import org.elasticsearch.common.resource.ResourceLaneBudget;
import org.elasticsearch.common.resource.ResourcePool;
import org.elasticsearch.common.resource.ResourcePriority;
import org.elasticsearch.common.resource.ResourceRejectedException;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

public class SearchAdmissionMetricsTests extends ESTestCase {

    public void testGaugesReflectPoolStats() {
        RecordingMeterRegistry meter = new RecordingMeterRegistry();
        ResourcePool pool = new ResourcePool("test", 10, 1000, 4, Map.of(ResourcePriority.HIGH, new ResourceLaneBudget(4, 0)));
        new SearchAdmissionMetrics(meter, pool::stats);

        Reservation reservation = pool.acquire(3, 100, ResourcePriority.HIGH);
        expectThrows(ResourceRejectedException.class, () -> pool.acquire(11, 0, ResourcePriority.NORMAL)); // bumps rejected total

        meter.getRecorder().collect();

        assertEquals(3L, scalar(meter, InstrumentType.LONG_GAUGE, "es.search.admission.slots.current_used"));
        assertEquals(7L, scalar(meter, InstrumentType.LONG_GAUGE, "es.search.admission.slots.current_available"));
        assertEquals(100L, scalar(meter, InstrumentType.LONG_GAUGE, "es.search.admission.memory.current_used"));
        assertEquals(0L, scalar(meter, InstrumentType.LONG_GAUGE, "es.search.admission.queue.current_size"));
        assertEquals(1L, scalar(meter, InstrumentType.LONG_ASYNC_COUNTER, "es.search.admission.rejected.total"));

        // Per-lane gauge: HIGH holds the 3 slots, the others none.
        List<Measurement> lanes = meter.getRecorder()
            .getMeasurements(InstrumentType.LONG_GAUGE, "es.search.admission.lane.current_used_slots");
        assertEquals(3L, laneSlots(lanes, ResourcePriority.HIGH));
        assertEquals(0L, laneSlots(lanes, ResourcePriority.NORMAL));

        reservation.close();
    }

    public void testDisabledSourceReportsZeroAndNoLanes() {
        RecordingMeterRegistry meter = new RecordingMeterRegistry();
        new SearchAdmissionMetrics(meter, () -> null);

        meter.getRecorder().collect();

        assertEquals(0L, scalar(meter, InstrumentType.LONG_GAUGE, "es.search.admission.slots.current_used"));
        assertEquals(0L, scalar(meter, InstrumentType.LONG_ASYNC_COUNTER, "es.search.admission.rejected.total"));
        assertTrue(meter.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, "es.search.admission.lane.current_used_slots").isEmpty());
    }

    private static long scalar(RecordingMeterRegistry meter, InstrumentType type, String name) {
        List<Measurement> measurements = meter.getRecorder().getMeasurements(type, name);
        assertFalse("no measurements for " + name, measurements.isEmpty());
        return measurements.get(measurements.size() - 1).getLong();
    }

    private static long laneSlots(List<Measurement> lanes, ResourcePriority lane) {
        return lanes.stream()
            .filter(m -> lane.name().equals(m.attributes().get("lane")))
            .mapToLong(Measurement::getLong)
            .reduce((a, b) -> b)
            .orElseThrow();
    }
}
