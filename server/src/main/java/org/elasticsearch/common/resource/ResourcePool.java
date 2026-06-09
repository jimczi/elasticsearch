/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.resource;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A local, node-scoped pool of capacity that callers reserve before doing bounded work and release when they are done.
 * It is the core primitive of the search resource manager: a single place that knows how much of a resource is in use,
 * admits work only while capacity remains, and otherwise queues or rejects — so a node never silently accepts more work
 * than it has budgeted for.
 *
 * <p>Capacity has two dimensions, both bounded together: <b>slots</b> (abstract units of concurrent work) and
 * <b>memory</b> (bytes of an admission-time memory <em>entitlement</em>; a circuit breaker remains the fine-grained
 * enforcer of actual bytes). A request asks for a positive number of slots plus a non-negative memory entitlement, in a
 * {@link ResourcePriority} lane, and is admitted only when both dimensions fit.
 *
 * <h2>Priority lanes</h2>
 * Each {@link ResourcePriority} is a lane with a guaranteed {@link ResourceLaneBudget floor} of slots and memory (zero by
 * default, which makes the pool behave as a single flat budget). Lanes may borrow idle capacity beyond their floor, but
 * capacity used above a lane's floor is <em>reclaimable</em>: when a lane is below its floor and has work waiting that
 * the free capacity cannot satisfy, the pool reclaims borrowed capacity from other lanes (lowest-priority first, never
 * dropping a lane below its own floor) by invoking those reservations' reclaim hooks. A reclaim hook is expected to
 * cancel the borrowing work so it releases its reservation; the freed capacity then drains to the starved lane. This is
 * cooperative preemption — the pool never force-releases another reservation's state. A lane's own floor is never
 * reclaimed, which guarantees minimum progress for lower-priority (e.g. unboosted) work.
 *
 * <h2>Admission</h2>
 * <ul>
 *   <li>{@link #tryAcquire} / {@link #acquire} — synchronous, non-blocking, no queuing and no reclaim. They succeed only
 *       if both dimensions fit <em>and no other request is already waiting</em>; otherwise they return {@code null} /
 *       throw.
 *   <li>{@link #acquireAsync} — grants inline when possible; otherwise triggers reclaim (if the request is below its
 *       lane floor), places the request on a bounded waiting queue, and notifies the listener later when capacity frees
 *       up, or rejects immediately if the queue is full.
 * </ul>
 *
 * <h2>Queue draining</h2>
 * When capacity is released, the pool drains its queue by precedence: requests whose lane is currently below its floor
 * first (to restore floors), then by priority, then FIFO. It grants the highest-precedence request that fits and stops
 * as soon as that request does not fit, so a starved or high-priority request is never bypassed by a lower one.
 *
 * <h2>Lifecycle</h2>
 * Every granted {@link Reservation} must be closed (including on failure and cancellation paths) or the pool leaks
 * capacity. {@link #close() Closing the pool} rejects everything still waiting; reservations already granted stay valid
 * and must still be closed.
 *
 * <p>This version favours a single lock guarding all state over a lock-free fast path: correctness and easy reasoning
 * come before throughput. The lock is held only for in-memory bookkeeping — listeners and reclaim hooks are always
 * invoked outside it.
 */
public final class ResourcePool implements Releasable {

    private static final ResourcePriority[] LANES = ResourcePriority.values();
    private static final int LANE_COUNT = LANES.length;

    private final String name;
    private final long slotCapacity;
    private final long memoryCapacity;
    private final int maxQueueLength;
    private final long[] floorSlots = new long[LANE_COUNT];
    private final long[] floorMemory = new long[LANE_COUNT];

    // Used only by the timeout-bearing acquireAsync overload; null when the pool was built without a scheduler.
    private final Scheduler scheduler;
    private final Executor timeoutExecutor;

    private final ReentrantLock lock = new ReentrantLock();

    // All fields below are guarded by {@link #lock}.
    private long usedSlots = 0;
    private long usedMemory = 0;
    private long peakUsedSlots = 0;
    private long peakUsedMemory = 0;
    private final long[] usedSlotsByLane = new long[LANE_COUNT];
    private final long[] usedMemoryByLane = new long[LANE_COUNT];
    private final long[] reclaimedByLane = new long[LANE_COUNT];
    private final long[] acquiredByLane = new long[LANE_COUNT]; // monotonic per-lane acquire count
    // Live reservations per lane, in acquisition order, so reclaim can pick the oldest borrowers first.
    private final List<LinkedHashSet<ReservationImpl>> liveByLane = new ArrayList<>(LANE_COUNT);
    private final List<PendingRequest> queue = new ArrayList<>();
    private long sequence = 0;
    private boolean closed = false;

    private long totalAcquired = 0;
    private long totalReleased = 0;
    private long totalRejected = 0;
    private long totalQueued = 0;
    private long totalQueueRejected = 0;
    private long totalCancelled = 0;
    private long totalTimedOut = 0;
    private long totalReclaimed = 0;

    private static final Releasable NOOP_CANCEL = () -> {};

    /**
     * Builds a single-lane pool (all floors zero) without a scheduler: the timeout-bearing
     * {@link #acquireAsync(long, long, ResourcePriority, TimeValue, ActionListener)} overload is unavailable.
     */
    public ResourcePool(String name, long slotCapacity, long memoryCapacity, int maxQueueLength) {
        this(name, slotCapacity, memoryCapacity, maxQueueLength, Map.of(), null, null);
    }

    /** Builds a pool with per-lane floors but no scheduler (the timeout overload is unavailable). */
    public ResourcePool(
        String name,
        long slotCapacity,
        long memoryCapacity,
        int maxQueueLength,
        Map<ResourcePriority, ResourceLaneBudget> floors
    ) {
        this(name, slotCapacity, memoryCapacity, maxQueueLength, floors, null, null);
    }

    /**
     * Builds a pool with per-lane floors that can time out queued requests.
     *
     * @param floors          per-lane guaranteed floors; lanes absent from the map default to {@link ResourceLaneBudget#NONE}.
     *                        The sum of floors across lanes may not exceed the pool capacity in either dimension.
     * @param scheduler       used to schedule queue-wait timeouts; required for the timeout-bearing
     *                        {@link #acquireAsync(long, long, ResourcePriority, TimeValue, ActionListener)} overload
     * @param timeoutExecutor where a timed-out request's {@code onFailure} runs (kept off the scheduler thread)
     */
    public ResourcePool(
        String name,
        long slotCapacity,
        long memoryCapacity,
        int maxQueueLength,
        Map<ResourcePriority, ResourceLaneBudget> floors,
        Scheduler scheduler,
        Executor timeoutExecutor
    ) {
        if (slotCapacity <= 0) {
            throw new IllegalArgumentException(
                "resource pool [" + name + "] slot capacity must be positive but was [" + slotCapacity + "]"
            );
        }
        if (memoryCapacity <= 0) {
            throw new IllegalArgumentException(
                "resource pool [" + name + "] memory capacity must be positive but was [" + memoryCapacity + "]"
            );
        }
        if (maxQueueLength < 0) {
            throw new IllegalArgumentException(
                "resource pool [" + name + "] max queue length must be non-negative but was [" + maxQueueLength + "]"
            );
        }
        long totalFloorSlots = 0;
        long totalFloorMemory = 0;
        for (Map.Entry<ResourcePriority, ResourceLaneBudget> entry : floors.entrySet()) {
            ResourceLaneBudget budget = entry.getValue();
            int lane = entry.getKey().ordinal();
            floorSlots[lane] = budget.slots();
            floorMemory[lane] = budget.memoryBytes();
            totalFloorSlots += budget.slots();
            totalFloorMemory += budget.memoryBytes();
        }
        if (totalFloorSlots > slotCapacity) {
            throw new IllegalArgumentException(
                "resource pool [" + name + "] sum of lane slot floors [" + totalFloorSlots + "] exceeds capacity [" + slotCapacity + "]"
            );
        }
        if (totalFloorMemory > memoryCapacity) {
            throw new IllegalArgumentException(
                "resource pool ["
                    + name
                    + "] sum of lane memory floors ["
                    + totalFloorMemory
                    + "] exceeds capacity ["
                    + memoryCapacity
                    + "]"
            );
        }
        this.name = name;
        this.slotCapacity = slotCapacity;
        this.memoryCapacity = memoryCapacity;
        this.maxQueueLength = maxQueueLength;
        this.scheduler = scheduler;
        this.timeoutExecutor = timeoutExecutor;
        for (int i = 0; i < LANE_COUNT; i++) {
            liveByLane.add(new LinkedHashSet<>());
        }
    }

    public String name() {
        return name;
    }

    public long slotCapacity() {
        return slotCapacity;
    }

    public long memoryCapacity() {
        return memoryCapacity;
    }

    /**
     * Attempts to reserve {@code slots} slots and {@code memoryBytes} of memory entitlement without blocking, queuing, or
     * reclaiming. Succeeds only if the queue is empty and both dimensions fit.
     *
     * @return the granted reservation, or {@code null} if capacity is unavailable right now. A {@code null} return is a
     *         non-committal poll result and is not counted as a rejection.
     */
    public Reservation tryAcquire(long slots, long memoryBytes, ResourcePriority priority) {
        ensureValidDemand(slots, memoryBytes);
        lock.lock();
        try {
            ensureOpen();
            return tryGrantInline(slots, memoryBytes, priority, null);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Like {@link #tryAcquire} but throws instead of returning {@code null}. The reservation may register a reclaim hook
     * so it can later be preempted to restore another lane's floor.
     *
     * @throws ResourceRejectedException if capacity cannot be granted immediately
     */
    public Reservation acquire(long slots, long memoryBytes, ResourcePriority priority) {
        return acquire(slots, memoryBytes, priority, null);
    }

    public Reservation acquire(long slots, long memoryBytes, ResourcePriority priority, Runnable onReclaim) {
        ensureValidDemand(slots, memoryBytes);
        Reservation reservation;
        lock.lock();
        try {
            ensureOpen();
            reservation = tryGrantInline(slots, memoryBytes, priority, onReclaim);
            if (reservation == null) {
                totalRejected++;
            }
        } finally {
            lock.unlock();
        }
        if (reservation == null) {
            throw rejection(slots, memoryBytes);
        }
        return reservation;
    }

    public Releasable acquireAsync(long slots, long memoryBytes, ResourcePriority priority, ActionListener<Reservation> listener) {
        return doAcquireAsync(slots, memoryBytes, priority, null, null, listener);
    }

    public Releasable acquireAsync(
        long slots,
        long memoryBytes,
        ResourcePriority priority,
        TimeValue timeout,
        ActionListener<Reservation> listener
    ) {
        return acquireAsync(slots, memoryBytes, priority, timeout, null, listener);
    }

    /**
     * Asynchronously reserves capacity, rejecting the request once it has waited {@code timeout} for capacity. If the
     * request is below its lane floor and cannot be granted immediately, the pool first reclaims borrowed capacity from
     * other lanes by invoking their reclaim hooks. The granted reservation registers {@code onReclaim} so it too can be
     * preempted later.
     *
     * @throws IllegalStateException if the pool was built without a scheduler
     */
    public Releasable acquireAsync(
        long slots,
        long memoryBytes,
        ResourcePriority priority,
        TimeValue timeout,
        Runnable onReclaim,
        ActionListener<Reservation> listener
    ) {
        if (scheduler == null) {
            throw new IllegalStateException("resource pool [" + name + "] was built without a scheduler; cannot honour acquire timeouts");
        }
        if (timeout == null) {
            throw new IllegalArgumentException("timeout must not be null");
        }
        return doAcquireAsync(slots, memoryBytes, priority, timeout, onReclaim, listener);
    }

    private Releasable doAcquireAsync(
        long slots,
        long memoryBytes,
        ResourcePriority priority,
        TimeValue timeout,
        Runnable onReclaim,
        ActionListener<Reservation> listener
    ) {
        ensureValidDemand(slots, memoryBytes);

        Reservation granted = null;
        PendingRequest pending = null;
        List<ReservationImpl> toReclaim = List.of();
        lock.lock();
        try {
            if (closed) {
                totalRejected++;
            } else if (slots > slotCapacity || memoryBytes > memoryCapacity) {
                // No amount of releasing can ever satisfy this request, so fail fast rather than queue it forever.
                totalRejected++;
            } else {
                granted = tryGrantInline(slots, memoryBytes, priority, onReclaim);
                if (granted == null) {
                    if (queue.size() >= maxQueueLength) {
                        totalRejected++;
                        totalQueueRejected++;
                    } else {
                        pending = new PendingRequest(slots, memoryBytes, priority, sequence++, onReclaim, listener);
                        queue.add(pending);
                        totalQueued++;
                        // If the request is owed by its lane floor, reclaim borrowed capacity from other lanes for it.
                        toReclaim = selectReclaimVictims(priority, slots, memoryBytes);
                        if (timeout != null && scheduleTimeout(pending, timeout) == false) {
                            // Scheduler is shutting down: we cannot guarantee the wait bound, so undo the enqueue and
                            // reject rather than queue a request that might wait forever.
                            queue.remove(pending);
                            totalQueued--;
                            totalRejected++;
                            pending = null;
                        }
                    }
                }
            }
        } finally {
            lock.unlock();
        }

        // Invoke reclaim hooks outside the lock; victims are expected to cancel their work and release, which drains to
        // the queued request.
        for (ReservationImpl victim : toReclaim) {
            victim.reclaim();
        }

        if (pending != null) {
            final PendingRequest queued = pending;
            return () -> cancel(queued);
        }
        if (granted != null) {
            listener.onResponse(granted);
        } else {
            listener.onFailure(closedOrRejected(slots, memoryBytes));
        }
        return NOOP_CANCEL;
    }

    /** @return a consistent snapshot of the pool's live gauges and lifetime counters, including the per-lane breakdown. */
    public ResourcePoolStats stats() {
        lock.lock();
        try {
            List<ResourceLaneStats> lanes = new ArrayList<>(LANE_COUNT);
            for (int i = 0; i < LANE_COUNT; i++) {
                lanes.add(
                    new ResourceLaneStats(
                        LANES[i],
                        floorSlots[i],
                        floorMemory[i],
                        usedSlotsByLane[i],
                        usedMemoryByLane[i],
                        Math.max(0, usedSlotsByLane[i] - floorSlots[i]),
                        Math.max(0, usedMemoryByLane[i] - floorMemory[i]),
                        reclaimedByLane[i],
                        acquiredByLane[i]
                    )
                );
            }
            return new ResourcePoolStats(
                slotCapacity,
                memoryCapacity,
                usedSlots,
                usedMemory,
                peakUsedSlots,
                peakUsedMemory,
                queue.size(),
                totalAcquired,
                totalReleased,
                totalRejected,
                totalQueued,
                totalQueueRejected,
                totalCancelled,
                totalTimedOut,
                totalReclaimed,
                List.copyOf(lanes)
            );
        } finally {
            lock.unlock();
        }
    }

    /**
     * Closes the pool: marks it closed and rejects every request still waiting on the queue. Reservations that have
     * already been granted remain valid and must still be closed to return their capacity.
     */
    @Override
    public void close() {
        final List<PendingRequest> toReject;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            toReject = drainQueueForRejection();
        } finally {
            lock.unlock();
        }
        for (PendingRequest request : toReject) {
            request.listener.onFailure(new ResourceRejectedException("resource pool [" + name + "] is closed"));
        }
    }

    // -- internals ---------------------------------------------------------------------------------------------------

    // Caller must hold the lock.
    private boolean fits(long slots, long memoryBytes) {
        return usedSlots + slots <= slotCapacity && usedMemory + memoryBytes <= memoryCapacity;
    }

    // Caller must hold the lock. Grants only if nothing is queued ahead and both dimensions fit globally.
    private Reservation tryGrantInline(long slots, long memoryBytes, ResourcePriority priority, Runnable onReclaim) {
        if (queue.isEmpty() && fits(slots, memoryBytes)) {
            return doReserve(priority, slots, memoryBytes, onReclaim);
        }
        return null;
    }

    // Caller must hold the lock.
    private ReservationImpl doReserve(ResourcePriority priority, long slots, long memoryBytes, Runnable onReclaim) {
        int lane = priority.ordinal();
        usedSlots += slots;
        usedMemory += memoryBytes;
        usedSlotsByLane[lane] += slots;
        usedMemoryByLane[lane] += memoryBytes;
        peakUsedSlots = Math.max(peakUsedSlots, usedSlots);
        peakUsedMemory = Math.max(peakUsedMemory, usedMemory);
        totalAcquired++;
        acquiredByLane[lane]++;
        ReservationImpl reservation = new ReservationImpl(priority, slots, memoryBytes, onReclaim);
        liveByLane.get(lane).add(reservation);
        return reservation;
    }

    // Releases a reservation's capacity and admits as many waiting requests as now fit. The freshly granted reservations
    // are returned so their listeners can be invoked outside the lock.
    private List<GrantedRequest> release(ReservationImpl reservation) {
        final List<GrantedRequest> granted;
        lock.lock();
        try {
            int lane = reservation.priority.ordinal();
            usedSlots -= reservation.slots;
            usedMemory -= reservation.memoryBytes;
            usedSlotsByLane[lane] -= reservation.slots;
            usedMemoryByLane[lane] -= reservation.memoryBytes;
            liveByLane.get(lane).remove(reservation);
            assert usedSlots >= 0 : "resource pool [" + name + "] released more slots than were held";
            assert usedMemory >= 0 : "resource pool [" + name + "] released more memory than was held";
            totalReleased++;
            granted = drainQueue();
        } finally {
            lock.unlock();
        }
        return granted;
    }

    // Caller must hold the lock. Grants the highest-precedence queued request that fits, repeatedly, stopping as soon as
    // the best candidate does not fit so it is not bypassed by a lower-precedence request.
    private List<GrantedRequest> drainQueue() {
        List<GrantedRequest> granted = null;
        while (queue.isEmpty() == false) {
            PendingRequest best = selectBest();
            if (fits(best.slots, best.memoryBytes) == false) {
                break;
            }
            queue.remove(best);
            cancelTimeout(best);
            ReservationImpl reservation = doReserve(best.priority, best.slots, best.memoryBytes, best.onReclaim);
            if (granted == null) {
                granted = new ArrayList<>();
            }
            granted.add(new GrantedRequest(best.listener, reservation));
        }
        return granted == null ? List.of() : granted;
    }

    // Caller must hold the lock. Highest precedence = lane currently below its floor first (to restore floors), then
    // higher priority, then oldest (lowest sequence).
    private PendingRequest selectBest() {
        PendingRequest best = null;
        boolean bestBelowFloor = false;
        for (PendingRequest candidate : queue) {
            boolean belowFloor = isLaneBelowFloor(candidate.priority.ordinal());
            if (best == null || betterThan(belowFloor, candidate, bestBelowFloor, best)) {
                best = candidate;
                bestBelowFloor = belowFloor;
            }
        }
        return best;
    }

    private static boolean betterThan(boolean aBelowFloor, PendingRequest a, boolean bBelowFloor, PendingRequest b) {
        if (aBelowFloor != bBelowFloor) {
            return aBelowFloor; // below-floor lanes win
        }
        if (a.priority != b.priority) {
            return a.priority.ordinal() > b.priority.ordinal(); // higher priority wins
        }
        return a.sequence < b.sequence; // FIFO within a class
    }

    // Caller must hold the lock.
    private boolean isLaneBelowFloor(int lane) {
        return usedSlotsByLane[lane] < floorSlots[lane] || usedMemoryByLane[lane] < floorMemory[lane];
    }

    // Caller must hold the lock. Picks borrowed reservations from other lanes to reclaim so the requesting lane can
    // reach its floor, preferring lowest-priority lanes and never dropping a victim lane below its own floor. Only
    // reservations that registered a reclaim hook are eligible. The chosen victims are marked so they are not picked
    // again, but their hooks are invoked by the caller outside the lock.
    private List<ReservationImpl> selectReclaimVictims(ResourcePriority priority, long slots, long memoryBytes) {
        int lane = priority.ordinal();
        long shortfallSlots = Math.max(0, slots - (slotCapacity - usedSlots));
        long shortfallMemory = Math.max(0, memoryBytes - (memoryCapacity - usedMemory));
        // Only reclaim to satisfy the floor-owed portion of the request, never to satisfy borrowing.
        long needSlots = Math.min(shortfallSlots, Math.max(0, floorSlots[lane] - usedSlotsByLane[lane]));
        long needMemory = Math.min(shortfallMemory, Math.max(0, floorMemory[lane] - usedMemoryByLane[lane]));
        if (needSlots <= 0 && needMemory <= 0) {
            return List.of();
        }

        List<ReservationImpl> victims = null;
        long freedSlots = 0;
        long freedMemory = 0;
        // Projected lane usage as we tentatively reclaim, so we never plan to drop a lane below its floor.
        long[] projectedSlots = usedSlotsByLane.clone();
        long[] projectedMemory = usedMemoryByLane.clone();
        for (int victimLane = 0; victimLane < LANE_COUNT && (freedSlots < needSlots || freedMemory < needMemory); victimLane++) {
            if (victimLane == lane) {
                continue;
            }
            for (ReservationImpl candidate : liveByLane.get(victimLane)) {
                if (freedSlots >= needSlots && freedMemory >= needMemory) {
                    break;
                }
                if (candidate.reclaimRequested || candidate.onReclaim == null) {
                    continue;
                }
                if (projectedSlots[victimLane] - candidate.slots < floorSlots[victimLane]
                    || projectedMemory[victimLane] - candidate.memoryBytes < floorMemory[victimLane]) {
                    continue; // reclaiming this one would breach the victim lane's own floor
                }
                candidate.reclaimRequested = true;
                projectedSlots[victimLane] -= candidate.slots;
                projectedMemory[victimLane] -= candidate.memoryBytes;
                freedSlots += candidate.slots;
                freedMemory += candidate.memoryBytes;
                reclaimedByLane[victimLane]++;
                totalReclaimed++;
                if (victims == null) {
                    victims = new ArrayList<>();
                }
                victims.add(candidate);
            }
        }
        return victims == null ? List.of() : victims;
    }

    // Caller must hold the lock.
    private List<PendingRequest> drainQueueForRejection() {
        if (queue.isEmpty()) {
            return List.of();
        }
        List<PendingRequest> rejected = new ArrayList<>(queue);
        for (PendingRequest request : rejected) {
            cancelTimeout(request);
        }
        queue.clear();
        return rejected;
    }

    private void cancel(PendingRequest request) {
        boolean removed;
        lock.lock();
        try {
            removed = queue.remove(request);
            if (removed) {
                totalCancelled++;
                cancelTimeout(request);
            }
        } finally {
            lock.unlock();
        }
        if (removed) {
            request.listener.onFailure(new ResourceRejectedException("request for [" + request.describeDemand() + "] was cancelled"));
        }
    }

    // Invoked by the scheduler when a queued request has waited too long. No-op if the request was already granted,
    // cancelled, or rejected (it will no longer be in the queue).
    private void timeout(PendingRequest request) {
        boolean removed;
        lock.lock();
        try {
            removed = queue.remove(request);
            if (removed) {
                totalRejected++;
                totalTimedOut++;
            }
        } finally {
            lock.unlock();
        }
        if (removed) {
            request.listener.onFailure(
                new ResourceRejectedException(
                    "request for [" + request.describeDemand() + "] timed out waiting for capacity in resource pool [" + name + "]"
                )
            );
        }
    }

    // Caller must hold the lock. Returns false if the scheduler refused the task (e.g. it is shutting down).
    private boolean scheduleTimeout(PendingRequest request, TimeValue timeout) {
        try {
            request.timeoutHandle = scheduler.schedule(() -> timeout(request), timeout, timeoutExecutor);
            return true;
        } catch (EsRejectedExecutionException e) {
            return false;
        }
    }

    private static void cancelTimeout(PendingRequest request) {
        if (request.timeoutHandle != null) {
            request.timeoutHandle.cancel();
        }
    }

    private void ensureValidDemand(long slots, long memoryBytes) {
        if (slots <= 0) {
            throw new IllegalArgumentException("resource pool [" + name + "] slots must be positive but was [" + slots + "]");
        }
        if (memoryBytes < 0) {
            throw new IllegalArgumentException(
                "resource pool [" + name + "] memory bytes must be non-negative but was [" + memoryBytes + "]"
            );
        }
    }

    // Caller must hold the lock.
    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("resource pool [" + name + "] is closed");
        }
    }

    private RuntimeException closedOrRejected(long slots, long memoryBytes) {
        lock.lock();
        try {
            if (closed) {
                return new ResourceRejectedException("resource pool [" + name + "] is closed");
            }
        } finally {
            lock.unlock();
        }
        return rejection(slots, memoryBytes);
    }

    private ResourceRejectedException rejection(long slots, long memoryBytes) {
        ResourcePoolStats snapshot = stats();
        return new ResourceRejectedException(
            Strings.format(
                "rejected request for [%d slots, %d memory bytes] from resource pool [%s] " + "[slots=%d/%d, memory=%d/%d, queue=%d/%d]",
                slots,
                memoryBytes,
                name,
                snapshot.currentUsedSlots(),
                slotCapacity,
                snapshot.currentUsedMemory(),
                memoryCapacity,
                snapshot.currentQueueLength(),
                maxQueueLength
            )
        );
    }

    private record GrantedRequest(ActionListener<Reservation> listener, Reservation reservation) {}

    private static final class PendingRequest {
        private final long slots;
        private final long memoryBytes;
        private final ResourcePriority priority;
        private final long sequence;
        private final Runnable onReclaim;
        private final ActionListener<Reservation> listener;
        // Set under the pool lock right after enqueue; cancelled when the request is granted, cancelled, or rejected.
        private Scheduler.ScheduledCancellable timeoutHandle;

        private PendingRequest(
            long slots,
            long memoryBytes,
            ResourcePriority priority,
            long sequence,
            Runnable onReclaim,
            ActionListener<Reservation> listener
        ) {
            this.slots = slots;
            this.memoryBytes = memoryBytes;
            this.priority = priority;
            this.sequence = sequence;
            this.onReclaim = onReclaim;
            this.listener = listener;
        }

        private String describeDemand() {
            return slots + " slots, " + memoryBytes + " memory bytes";
        }
    }

    private final class ReservationImpl implements Reservation {
        private final ResourcePriority priority;
        private final long slots;
        private final long memoryBytes;
        private final Runnable onReclaim;
        private boolean released = false;
        // Set true (under the pool lock) when this reservation has been picked for reclaim, so it is not picked twice.
        private boolean reclaimRequested = false;

        private ReservationImpl(ResourcePriority priority, long slots, long memoryBytes, Runnable onReclaim) {
            this.priority = priority;
            this.slots = slots;
            this.memoryBytes = memoryBytes;
            this.onReclaim = onReclaim;
        }

        @Override
        public long slots() {
            return slots;
        }

        @Override
        public long memoryBytes() {
            return memoryBytes;
        }

        @Override
        public ResourcePriority priority() {
            return priority;
        }

        // Invoked outside the lock. Asks the holder to release this reservation so its borrowed capacity can be
        // reclaimed; the holder is expected to cancel its work, which closes the reservation.
        private void reclaim() {
            if (onReclaim != null) {
                onReclaim.run();
            }
        }

        @Override
        public void close() {
            // close() is idempotent: only the first call returns capacity. The released flag is guarded by the pool
            // lock so that a concurrent double-close cannot release the same capacity twice.
            lock.lock();
            final boolean firstClose;
            try {
                firstClose = released == false;
                if (firstClose) {
                    released = true;
                }
            } finally {
                lock.unlock();
            }
            if (firstClose == false) {
                return;
            }
            List<GrantedRequest> granted = release(this);
            for (GrantedRequest g : granted) {
                g.listener().onResponse(g.reservation());
            }
        }
    }
}
