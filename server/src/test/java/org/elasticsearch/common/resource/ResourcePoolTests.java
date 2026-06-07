/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.resource;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class ResourcePoolTests extends ESTestCase {

    // Convenience for slot-focused tests: an effectively unbounded memory dimension so only slots bind.
    private static ResourcePool slotPool(long slotCapacity, int maxQueueLength) {
        return new ResourcePool("test", slotCapacity, Long.MAX_VALUE, maxQueueLength);
    }

    public void testAcquireAndReleaseTracksUsage() {
        ResourcePool pool = slotPool(10, 0);
        Reservation r1 = pool.acquire(4, 0, ResourcePriority.NORMAL);
        assertEquals(4, r1.slots());
        assertEquals(6, pool.stats().currentAvailableSlots());

        Reservation r2 = pool.acquire(6, 0, ResourcePriority.NORMAL);
        assertEquals(0, pool.stats().currentAvailableSlots());

        r1.close();
        assertEquals(4, pool.stats().currentAvailableSlots());
        r2.close();
        assertEquals(10, pool.stats().currentAvailableSlots());

        ResourcePoolStats stats = pool.stats();
        assertEquals(2, stats.totalAcquired());
        assertEquals(2, stats.totalReleased());
        assertEquals(10, stats.peakUsedSlots());
    }

    public void testTryAcquireReturnsNullWhenInsufficientCapacity() {
        ResourcePool pool = slotPool(10, 0);
        Reservation r = pool.acquire(8, 0, ResourcePriority.NORMAL);
        assertNull(pool.tryAcquire(3, 0, ResourcePriority.NORMAL));
        // A null poll is not counted as a rejection.
        assertEquals(0, pool.stats().totalRejected());
        r.close();
        assertNotNull(pool.tryAcquire(3, 0, ResourcePriority.NORMAL));
    }

    public void testAcquireThrowsWhenFull() {
        ResourcePool pool = slotPool(10, 0);
        pool.acquire(10, 0, ResourcePriority.NORMAL);
        expectThrows(ResourceRejectedException.class, () -> pool.acquire(1, 0, ResourcePriority.NORMAL));
        assertEquals(1, pool.stats().totalRejected());
    }

    public void testMemoryDimensionBindsIndependentlyOfSlots() {
        // Plenty of slots, but only 100 bytes of memory budget.
        ResourcePool pool = new ResourcePool("test", 100, 100, 0);
        Reservation r = pool.acquire(1, 80, ResourcePriority.NORMAL);
        assertEquals(80, r.memoryBytes());
        assertEquals(20, pool.stats().currentAvailableMemory());

        // A slot is free, but not enough memory entitlement remains.
        assertNull(pool.tryAcquire(1, 30, ResourcePriority.NORMAL));
        // Memory fits now.
        Reservation r2 = pool.acquire(1, 20, ResourcePriority.NORMAL);
        assertEquals(0, pool.stats().currentAvailableMemory());

        r.close();
        r2.close();
        assertEquals(100, pool.stats().currentAvailableMemory());
    }

    public void testRequestExceedingMemoryCapacityIsRejected() {
        ResourcePool pool = new ResourcePool("test", 100, 100, 4);
        PlainActionFuture<Reservation> future = new PlainActionFuture<>();
        pool.acquireAsync(1, 101, ResourcePriority.NORMAL, future);
        assertTrue(future.isDone());
        expectThrows(ResourceRejectedException.class, future::actionGet);
        assertEquals(0, pool.stats().totalQueued());
        assertEquals(1, pool.stats().totalRejected());
    }

    public void testTryAcquireRespectsWaitingQueue() {
        ResourcePool pool = slotPool(10, 4);
        Reservation r = pool.acquire(8, 0, ResourcePriority.NORMAL); // 2 slots free
        PlainActionFuture<Reservation> queued = new PlainActionFuture<>();
        pool.acquireAsync(5, 0, ResourcePriority.NORMAL, queued); // cannot fit, queues
        assertFalse(queued.isDone());

        // Even though 2 slots are free, a fresh request must not barge ahead of the waiting one.
        assertNull(pool.tryAcquire(2, 0, ResourcePriority.NORMAL));
        r.close();
        assertTrue(queued.isDone());
    }

    public void testAsyncGrantsInlineWhenCapacityAvailable() {
        ResourcePool pool = slotPool(10, 4);
        PlainActionFuture<Reservation> future = new PlainActionFuture<>();
        pool.acquireAsync(5, 0, ResourcePriority.NORMAL, future);
        assertTrue(future.isDone());
        assertEquals(5, future.actionGet().slots());
    }

    public void testAsyncQueuesAndDrainsOnRelease() {
        ResourcePool pool = slotPool(10, 4);
        Reservation r = pool.acquire(10, 0, ResourcePriority.NORMAL);

        PlainActionFuture<Reservation> future = new PlainActionFuture<>();
        pool.acquireAsync(6, 0, ResourcePriority.NORMAL, future);
        assertFalse(future.isDone());
        assertEquals(1, pool.stats().currentQueueLength());
        assertEquals(1, pool.stats().totalQueued());

        r.close();
        assertTrue(future.isDone());
        assertEquals(6, future.actionGet().slots());
        assertEquals(0, pool.stats().currentQueueLength());
    }

    public void testFullQueueRejects() {
        ResourcePool pool = slotPool(10, 1);
        pool.acquire(10, 0, ResourcePriority.NORMAL);

        PlainActionFuture<Reservation> queued = new PlainActionFuture<>();
        pool.acquireAsync(1, 0, ResourcePriority.NORMAL, queued); // fills the single queue slot
        assertFalse(queued.isDone());

        PlainActionFuture<Reservation> rejected = new PlainActionFuture<>();
        pool.acquireAsync(1, 0, ResourcePriority.NORMAL, rejected);
        assertTrue(rejected.isDone());
        expectThrows(ResourceRejectedException.class, rejected::actionGet);

        ResourcePoolStats stats = pool.stats();
        assertEquals(1, stats.totalQueueRejected());
        assertEquals(1, stats.totalRejected());
    }

    public void testQueueDrainsInStrictPriorityOrder() {
        ResourcePool pool = slotPool(10, 8);
        Reservation held = pool.acquire(10, 0, ResourcePriority.NORMAL); // pool full; everything queues

        // Enqueue out of priority order; each needs the full capacity so only one can be granted at a time.
        List<ResourcePriority> grantOrder = new ArrayList<>();
        PlainActionFuture<Reservation> low = recordingFuture(grantOrder, ResourcePriority.LOW);
        PlainActionFuture<Reservation> normal = recordingFuture(grantOrder, ResourcePriority.NORMAL);
        PlainActionFuture<Reservation> high = recordingFuture(grantOrder, ResourcePriority.HIGH);
        pool.acquireAsync(10, 0, ResourcePriority.LOW, low);
        pool.acquireAsync(10, 0, ResourcePriority.NORMAL, normal);
        pool.acquireAsync(10, 0, ResourcePriority.HIGH, high);

        held.close();
        high.actionGet().close();
        normal.actionGet().close();
        low.actionGet().close();

        assertEquals(List.of(ResourcePriority.HIGH, ResourcePriority.NORMAL, ResourcePriority.LOW), grantOrder);
    }

    public void testSlotsExceedingCapacityIsRejectedImmediately() {
        ResourcePool pool = slotPool(10, 4);
        PlainActionFuture<Reservation> future = new PlainActionFuture<>();
        pool.acquireAsync(11, 0, ResourcePriority.NORMAL, future);
        assertTrue(future.isDone());
        expectThrows(ResourceRejectedException.class, future::actionGet);
        // It must not have been queued, since releasing could never satisfy it.
        assertEquals(0, pool.stats().totalQueued());
        assertEquals(1, pool.stats().totalRejected());
    }

    public void testCancelQueuedRequest() {
        ResourcePool pool = slotPool(10, 4);
        Reservation held = pool.acquire(10, 0, ResourcePriority.NORMAL);

        PlainActionFuture<Reservation> future = new PlainActionFuture<>();
        Releasable cancel = pool.acquireAsync(5, 0, ResourcePriority.NORMAL, future);
        assertFalse(future.isDone());

        cancel.close();
        assertTrue(future.isDone());
        expectThrows(ResourceRejectedException.class, future::actionGet);
        assertEquals(1, pool.stats().totalCancelled());

        // Releasing now must not try to grant the cancelled request.
        held.close();
        assertEquals(10, pool.stats().currentAvailableSlots());
        assertEquals(0, pool.stats().currentQueueLength());
    }

    public void testCloseIsIdempotentOnReservation() {
        ResourcePool pool = slotPool(10, 0);
        Reservation r = pool.acquire(4, 0, ResourcePriority.NORMAL);
        r.close();
        r.close(); // no-op, must not double-release
        assertEquals(10, pool.stats().currentAvailableSlots());
        assertEquals(1, pool.stats().totalReleased());
    }

    public void testClosingPoolRejectsQueuedRequests() {
        ResourcePool pool = slotPool(10, 4);
        pool.acquire(10, 0, ResourcePriority.NORMAL);
        PlainActionFuture<Reservation> future = new PlainActionFuture<>();
        pool.acquireAsync(5, 0, ResourcePriority.NORMAL, future);
        assertFalse(future.isDone());

        pool.close();
        assertTrue(future.isDone());
        expectThrows(ResourceRejectedException.class, future::actionGet);
    }

    public void testQueuedRequestTimesOut() {
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        ThreadPool threadPool = taskQueue.getThreadPool();
        ResourcePool pool = new ResourcePool("test", 10, Long.MAX_VALUE, 4, Map.of(), threadPool, threadPool.generic());

        pool.acquire(10, 0, ResourcePriority.NORMAL);
        PlainActionFuture<Reservation> future = new PlainActionFuture<>();
        pool.acquireAsync(5, 0, ResourcePriority.NORMAL, TimeValue.timeValueSeconds(30), future);
        assertFalse(future.isDone());

        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();

        assertTrue(future.isDone());
        expectThrows(ResourceRejectedException.class, future::actionGet);
        ResourcePoolStats stats = pool.stats();
        assertEquals(1, stats.totalTimedOut());
        assertEquals(1, stats.totalRejected());
        assertEquals(0, stats.currentQueueLength());
    }

    public void testTimeoutIsCancelledWhenRequestIsGranted() {
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        ThreadPool threadPool = taskQueue.getThreadPool();
        ResourcePool pool = new ResourcePool("test", 10, Long.MAX_VALUE, 4, Map.of(), threadPool, threadPool.generic());

        Reservation held = pool.acquire(10, 0, ResourcePriority.NORMAL);
        PlainActionFuture<Reservation> future = new PlainActionFuture<>();
        pool.acquireAsync(5, 0, ResourcePriority.NORMAL, TimeValue.timeValueSeconds(30), future);
        assertFalse(future.isDone());

        held.close(); // grants the queued request before its timeout fires
        assertTrue(future.isDone());
        assertEquals(5, future.actionGet().slots());

        // Advancing past the timeout must not turn the granted request into a failure.
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertEquals(0, pool.stats().totalTimedOut());
    }

    public void testTimeoutOverloadRequiresScheduler() {
        ResourcePool pool = slotPool(10, 4);
        expectThrows(
            IllegalStateException.class,
            () -> pool.acquireAsync(1, 0, ResourcePriority.NORMAL, TimeValue.timeValueSeconds(1), new PlainActionFuture<>())
        );
    }

    public void testRejectsInvalidDemand() {
        ResourcePool pool = slotPool(10, 0);
        expectThrows(IllegalArgumentException.class, () -> pool.acquire(0, 0, ResourcePriority.NORMAL));
        expectThrows(IllegalArgumentException.class, () -> pool.acquire(-1, 0, ResourcePriority.NORMAL));
        expectThrows(IllegalArgumentException.class, () -> pool.acquire(1, -1, ResourcePriority.NORMAL));
    }

    public void testReclaimRestoresLaneFloor() {
        // HIGH lane is guaranteed 5 of 10 slots; LOW borrows everything.
        ResourcePool pool = new ResourcePool("test", 10, Long.MAX_VALUE, 4, Map.of(ResourcePriority.HIGH, new ResourceLaneBudget(5, 0)));

        AtomicReference<Reservation> lowHolder = new AtomicReference<>();
        Reservation low = pool.acquire(10, 0, ResourcePriority.LOW, () -> lowHolder.get().close());
        lowHolder.set(low);
        assertEquals(0, pool.stats().currentAvailableSlots());

        // HIGH asks for its floor; it cannot fit, so reclaim cancels the LOW borrower and HIGH is then granted.
        PlainActionFuture<Reservation> high = new PlainActionFuture<>();
        pool.acquireAsync(5, 0, ResourcePriority.HIGH, high);

        assertTrue(high.isDone());
        assertEquals(5, high.actionGet().slots());
        assertEquals(1, pool.stats().totalReclaimed());
    }

    public void testReclaimProtectsVictimFloor() {
        // LOW is guaranteed 4 slots; HIGH is guaranteed 3. Total capacity 10.
        ResourcePool pool = new ResourcePool(
            "test",
            10,
            Long.MAX_VALUE,
            4,
            Map.of(ResourcePriority.LOW, new ResourceLaneBudget(4, 0), ResourcePriority.HIGH, new ResourceLaneBudget(3, 0))
        );

        // LOW holds its floor (4) in one reservation and borrows 2 more in a second; both are reclaimable.
        Reservation lowFloor = pool.acquire(4, 0, ResourcePriority.LOW, () -> fail("the floor reservation must not be reclaimed"));
        AtomicReference<Reservation> lowBorrowHolder = new AtomicReference<>();
        Reservation lowBorrow = pool.acquire(2, 0, ResourcePriority.LOW, () -> lowBorrowHolder.get().close());
        lowBorrowHolder.set(lowBorrow);
        // NORMAL borrows the remaining 4.
        AtomicReference<Reservation> normalHolder = new AtomicReference<>();
        Reservation normal = pool.acquire(4, 0, ResourcePriority.NORMAL, () -> normalHolder.get().close());
        normalHolder.set(normal);
        assertEquals(0, pool.stats().currentAvailableSlots());

        // HIGH asks for its 3-slot floor. Reclaim must free 3 from borrowed capacity without touching LOW's floor.
        PlainActionFuture<Reservation> high = new PlainActionFuture<>();
        pool.acquireAsync(3, 0, ResourcePriority.HIGH, high);

        assertTrue(high.isDone());
        assertEquals(3, high.actionGet().slots());
        // LOW's floor reservation is still held (its reclaim hook would have failed the test).
        assertEquals(4, pool.stats().lanes().get(ResourcePriority.LOW.ordinal()).usedSlots());

        lowFloor.close();
        high.actionGet().close();
    }

    public void testNonReclaimableReservationIsNotPreempted() {
        ResourcePool pool = new ResourcePool("test", 10, Long.MAX_VALUE, 4, Map.of(ResourcePriority.HIGH, new ResourceLaneBudget(5, 0)));
        // LOW borrows everything but registers no reclaim hook, so it cannot be preempted.
        pool.acquire(10, 0, ResourcePriority.LOW);

        PlainActionFuture<Reservation> high = new PlainActionFuture<>();
        pool.acquireAsync(5, 0, ResourcePriority.HIGH, high);

        assertFalse(high.isDone());
        assertEquals(0, pool.stats().totalReclaimed());
        assertEquals(1, pool.stats().currentQueueLength());
    }

    public void testBorrowingRequestDoesNotTriggerReclaim() {
        // No floors: every request is pure borrowing, so a queued request must not preempt anyone.
        ResourcePool pool = slotPool(10, 4);
        pool.acquire(10, 0, ResourcePriority.HIGH, () -> fail("pure-borrow contention must not reclaim"));

        PlainActionFuture<Reservation> low = new PlainActionFuture<>();
        pool.acquireAsync(5, 0, ResourcePriority.LOW, low);

        assertFalse(low.isDone());
        assertEquals(0, pool.stats().totalReclaimed());
    }

    public void testFloorAwareDrainPrefersStarvedLane() {
        // LOW guaranteed 5 of 10. Pool full with NORMAL borrowers; both a NORMAL borrow and a LOW floor request wait.
        ResourcePool pool = new ResourcePool("test", 10, Long.MAX_VALUE, 8, Map.of(ResourcePriority.LOW, new ResourceLaneBudget(5, 0)));
        Reservation a = pool.acquire(5, 0, ResourcePriority.NORMAL);
        Reservation b = pool.acquire(5, 0, ResourcePriority.NORMAL);

        // Queue a NORMAL borrow first (lower precedence) and a LOW floor request second.
        PlainActionFuture<Reservation> normalBorrow = new PlainActionFuture<>();
        pool.acquireAsync(5, 0, ResourcePriority.NORMAL, normalBorrow);
        PlainActionFuture<Reservation> lowFloor = new PlainActionFuture<>();
        pool.acquireAsync(5, 0, ResourcePriority.LOW, lowFloor);

        // Free 5 slots: the LOW floor request must win even though NORMAL is higher priority and queued earlier.
        a.close();
        assertTrue(lowFloor.isDone());
        assertFalse(normalBorrow.isDone());

        b.close();
        assertTrue(normalBorrow.isDone());
    }

    public void testLaneStatsReportFloorsAndBorrowing() {
        ResourcePool pool = new ResourcePool("test", 10, Long.MAX_VALUE, 4, Map.of(ResourcePriority.HIGH, new ResourceLaneBudget(4, 0)));
        pool.acquire(6, 0, ResourcePriority.HIGH); // 4 floor + 2 borrowed
        ResourceLaneStats high = pool.stats().lanes().get(ResourcePriority.HIGH.ordinal());
        assertEquals(4, high.floorSlots());
        assertEquals(6, high.usedSlots());
        assertEquals(2, high.borrowedSlots());
    }

    private static PlainActionFuture<Reservation> recordingFuture(List<ResourcePriority> grantOrder, ResourcePriority priority) {
        PlainActionFuture<Reservation> future = new PlainActionFuture<>() {
            @Override
            public void onResponse(Reservation reservation) {
                grantOrder.add(priority);
                super.onResponse(reservation);
            }
        };
        return future;
    }
}
