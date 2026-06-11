# Search Resource Manager — Design & Implementation

> A node-local, distributed-aware admission and resource-control layer that lets `_search` and ES|QL share **one
> bounded, configurable budget**, so a node only starts queries whose resources it can actually control.

---

## TL;DR

Elasticsearch today accepts essentially **unbounded** concurrent search work: every `_search` and ES|QL request is
admitted at the coordinator, fanned out to data nodes, and only pushed back *reactively* by the shared circuit breakers
once memory is already high. There is no per-query memory isolation and no notion of "this node can only run N searches
at once."

We built a **search resource manager**: a small, composable layer that

1. **bounds concurrency** with a two-dimensional admission pool (execution **slots** ⟂ memory **entitlement**) that every
   shard search — `_search` *and* ES|QL — passes through;
2. **isolates memory per query** with a capped child circuit breaker, so a query that exceeds *its* budget fails alone
   instead of tripping the node;
3. turns acceptance into a **contract** via distributed admission — the coordinator reserves capacity on every
   participating node *before* the query runs, so "accepted" means "guaranteed to run" rather than "might fail a minute
   in";
4. organizes capacity into **priority lanes** with floors + borrowing + **preemptive reclaim**, and routes work into them
   by policy (system indices, **boosted/unboosted tiers**) so different classes of work get predictable shares — a boosted
   query can **preempt** an unboosted borrower (cancelling only its shard, re-run as a retryable rejection so it still
   finishes) down to the unboosted lane's floor, with an age gate so short work is left alone;
5. is **observable** — per-node, per-lane, per-layer stats over a REST endpoint and OTel metrics;
6. is **off by default**, fully **configurable**, and **mixed-version safe**.

Everything is gated behind `search.admission_control.*` settings and was validated with unit tests, integration tests,
and a live load-testing dashboard that drives real traffic against a running node and visualizes each layer reacting.

---

## 1. Goals

The north star: **a node should only start queries whose resource use it can control.** Concretely:

- **Graceful rejection over late failure.** Reject a query *up front* when the node is full, instead of accepting it and
  OOM-ing or tripping the global breaker halfway through.
- **Predictable behavior over maximum throughput.** Bounded, explicit budgets beat opportunistic over-commitment.
- **Per-query accountability.** Know — and cap — what each query costs, so one heavy query cannot take down a node.
- **A real contract at acceptance.** In a distributed engine a query spans many nodes; accepting it at the REST layer
  should mean every participating node has already agreed to run it.
- **One layer for both engines.** `_search` and ES|QL must share the same budget and the same controls — a *search
  management system* an administrator configures once.
- **Configurable, with real limits.** On-prem / ECH administrators set their own limits; in serverless *we* are the
  administrator. This is also a precursor to multi-project (several projects sharing one node's budget).
- **Incremental and safe.** Off by default, composes with (does not replace) the existing circuit breakers, ships behind
  settings, and is mixed-version safe.

### Non-goals (for now)
Spill-to-disk / compaction / partial results (the per-query breaker is the *place* to add them later), plan-time memory
estimation, and cross-cluster/cross-project nested admission (designed for, not yet built).

---

## 2. Background — what exists today, and the gap

| Mechanism | What it does | Why it's not enough |
|---|---|---|
| **Circuit breakers** (REQUEST 60% heap, parent 95% real heap) | Reactively trip when memory is already high | Node-global, *reactive*, no per-query view, trips late |
| **Search thread pool + queue** | Bounds threads, queues the rest | `_search` is **not** bounded by it — every shard request is accepted; ES\|QL drivers are separate |
| **IndexingPressure** | Heap-fraction admission for *indexing* (atomic reserve/reject/release, ~10% heap) | The precedent we mirror — but there is no search equivalent |

The search path had **no admission and no per-query memory isolation.** Both engines allocate through the shared REQUEST
breaker; neither accounts memory per query. That is the gap this work fills, using IndexingPressure as the conceptual
template (heap-fraction reservation, atomic accounting) extended to two dimensions and a distributed contract.

---

## 3. The model

Three orthogonal ideas:

```
                    ┌─────────────────────────────────────────────┐
   a search →       │   SLOTS  (CPU-bound, f(cores))               │   two dimensions, reserved together
                    │   MEMORY (heap-bound, f(heap))               │
                    └─────────────────────────────────────────────┘
                                       ×
                    ┌─────────────────────────────────────────────┐
                    │   LANES: SYSTEM > HIGH > NORMAL > LOW        │   per-lane floors + borrow idle + reclaim
                    └─────────────────────────────────────────────┘
                                       ×
                    ┌─────────────────────────────────────────────┐
                    │   SCOPE: node-local pool  ⟷  distributed lease  │
                    └─────────────────────────────────────────────┘
```

- **Two dimensions.** Execution **slots** model CPU concurrency (capacity = `shard_slots_per_thread × search_threads`);
  **memory entitlement** models heap. They are reserved together but bind independently — a query can be admitted on
  memory but queued on slots, or vice-versa.
- **Lanes.** Capacity is partitioned into priority lanes (`SYSTEM > HIGH > NORMAL > LOW`), each with a guaranteed
  **floor**, the ability to **borrow** idle capacity from other lanes, and **preemptive reclaim** when a higher lane needs
  its floor back. The lane is the priority. Work is *routed* into a lane by policy (system / boosted / unboosted) — see §7.
- **Scope.** The same reservation primitive serves both the **node-local** pool (a node protecting itself) and a
  **distributed lease** (a coordinator reserving on a remote node).

### Budget, not prediction
We deliberately do **not** try to predict a query's memory up front. The budget is a **contract + enforcement**:

```
   Σ per-query entitlements  ≤  M (node search budget ≈ f(heap), like IndexingPressure's 10%)  ≤  REQUEST breaker
```

Safety comes from the invariant `Σ E ≤ M ≤ breaker`, not from a forecast. Per-query entitlement `E = M / target_concurrency`
is a policy knob. (Design notes in `.agents/node-budget-design.md`.)

---

## 4. Core primitives (`server/.../common/resource/`)

| Class | Responsibility |
|---|---|
| `ResourcePool` | The 2-D admission pool. Single lock; per-lane floors/used; flat admission + reclaim-to-restore-floors + floor-aware drain; `tryAcquire` / `acquire` / `acquireAsync` (timeout + optional `onReclaim`); bounded queue; idempotent `Reservation`s; per-lane stats. |
| `ResourcePriority` | The lanes: `LOW, NORMAL, HIGH, SYSTEM` (SYSTEM highest precedence; isolated system-index lane). |
| `QueryMemoryBreaker` | A per-query `CircuitBreaker` with its **own** limit layered over a parent (the node REQUEST breaker). Every byte is checked against the query budget first, then charged through to the parent — so a query over budget fails query-scoped, the parent still sees every byte (no double counting), and `close()` reconciles any leak. |
| `QueryMemoryBreakerService` | A thin `CircuitBreakerService` that hands aggregations/BigArrays the per-query breaker instead of the shared one. |
| `SearchLaneResolver` | The pluggable **policy** mapping a unit of work → lane: `NORMAL_ONLY` (default), `SYSTEM_AWARE` (system→SYSTEM), `TIER` (system→SYSTEM, boosted→HIGH, unboosted→LOW). The pool is mechanism; lane assignment is policy. |
| `ResourcePoolStats` / `ResourceLaneStats` | Immutable, `Writeable` + `ToXContent` snapshots (slots, memory, queue, per-lane floors/used/borrowed/reclaimed/acquired, lifetime counters). |

**Reclaim model = "minimum threads, steal when free."** Each lane is guaranteed its floor; above the floor it borrows
idle capacity; when a higher-priority lane needs its floor and cannot be served otherwise, the pool reclaims the
lowest-priority *borrowed* reservations first — never breaching a victim lane's floor, only reservations that registered a
reclaim hook, and (with the age gate, §7) only those older than `reclaim_min_age`. The reclaim hook **preempts** the
borrower; §7 covers what that means for search work and how it stays retryable.

---

## 5. Node-local admission — both engines through one pool

`SearchService` owns the pool and exposes one entry point:

- **`admitSearchWork(slots, priority, onReclaim, executor, listener)`** → a `SearchAdmission(memoryBreaker, releasable)`.
  The reservation may be granted inline, after a short wait in the bounded queue, or **rejected** (queue full / timed out)
  → surfaced to the coordinator as a shard failure. The optional `onReclaim` hook lets a higher lane preempt it (§7).
- **`_search`** flows through `SearchService.runAsync(...)`: it reserves a slot before the shard task runs and releases
  it when the task settles. This is where `_search` becomes bounded for the first time.
- **ES|QL** flows through `DataNodeComputeHandler.runBatch` → `admitSearchWork` per batch of shards, so data-node compute
  shares the *same* budget as classic `_search`.

### Per-query memory enforcement
When a memory budget is configured, aggregations (and ES|QL drivers / node reduction) allocate through a
`QueryMemoryBreaker` sized to the query's entitlement. A query that exceeds its budget gets a **query-scoped**
`CircuitBreakingException` (HTTP 429) — only that query fails; the node keeps running. This is the granular isolation the
global breaker cannot provide, and the seam where spill/compact/partial-results will later attach.

---

## 6. The distributed contract — "accept = guaranteed to run"

A query spans many nodes; accepting it at REST should be a promise. So the **coordinator reserves capacity on every
participating node before the query runs.**

```
  REST accept ──► coordinator computes per-node demand ──► reserve on ALL nodes (all-or-nothing)
                         │                                          │
                         │  any node can't admit?                   │ all admitted
                         ▼                                          ▼
                 roll back, queue & retry  ──► accept deadline ──► run; data nodes skip their
                         │                       exceeded            own acquire (covered by lease)
                         ▼                                          │
                  reject up front (429)                            ▼
                                                          release every lease on completion
```

**Components (`server/.../search/admission/`):**

- `SearchAdmissionService` — node-local **lease** registry + the `internal:admission/search/*` reserve/release transport
  handlers; a leak backstop releases a coordinator's leases if it disconnects.
- `CoordinatorSearchAdmission` — the coordinator state machine: strict **all-or-nothing** reserve across nodes, rollback +
  retry within a bounded accept queue until a deadline, then reject. Partial reservations are never held across a wait,
  so two big searches grabbing disjoint node subsets can't deadlock.
- **Lease coverage** — the lease id is the coordinator search **task id**. A data node, seeing shard work whose parent
  task is covered by a lease, **skips its own local acquire** (the coordinator already paid for it) → no double counting.

### Wiring
- **`_search`**: `TransportSearchAction` computes per-node demand from the resolved shard iterators (local shards only;
  remote/CCS shards are admitted by their own cluster) and admits before running the phases (`runSearchPhases`).
- **ES|QL**: because ES|QL resolves nodes *incrementally* (with retries) and inserts a per-node group task on the
  partial-results path, the natural fit is a **per-node reserve at dispatch**, keyed by the per-node **child session id**:
  `DataNodeComputeHandler.sendRequest` reserves on the node before opening the exchange and releases when that node's
  compute settles; the data node looks the lease up by session id and runs under it.

### Execution under lease (memory follows the contract)
When a shard is covered by a lease, it runs under the lease's per-query breaker — the budget the coordinator reserved on
that node, **shared across all the query's shards there**:

- `_search`: `DefaultSearchContext.circuitBreaker()` returns the lease breaker, so the **whole shard** (query
  construction, the memory-accounting buffer, and aggregations) is bounded by it.
- ES|QL: `runBatch`/node reduction run under the lease breaker instead of acquiring per batch.

---

## 7. Lane routing and preemption

The floors/borrow/reclaim machinery is inert until something (a) assigns work to lanes other than `NORMAL`, and
(b) actually reclaims a borrowed slot when a higher lane needs it. This section covers both — routing, the preemption
design choice, the mechanism, how preempted work still finishes, and the age gate that keeps it from thrashing.

### 7.1 Routing work into lanes
The **`SearchLaneResolver`** seam maps work → lane, selected by `search.admission_control.lane_strategy`:

- `none` (default) — everything `NORMAL`; lanes inert (zero behavior change).
- `system` — system-index searches → `SYSTEM`.
- `tier` — system → `SYSTEM`, **`boosted` → `HIGH`**, **`unboosted` → `LOW`**, the rest → `NORMAL`.

The tier comes from a per-index setting **`index.search.boost_tier`** (`boosted` / `unboosted`) — the serverless
"search power" model, set by the administrator. The resolver's `Work` description (system flag + tier) is meant to grow
(data-source kind, project) without touching the pool. The lane is computed at **every admission point** from the indices
being searched (highest-priority lane wins for a multi-index query): `SearchService.laneFor(shard)` for local `_search`,
`SearchService.laneForIndex(indexMetadata)` for the ES|QL data node (`DataNodeComputeHandler.laneFor(batch)`) and the
coordinator reserve (`TransportSearchAction.computeAdmissionLane`). Per-lane floors are configured via
`search.admission_control.{boosted,unboosted,system}.slot_floor_fraction`.

### 7.2 The design choice: borrow, wait, or preempt
Honoring a higher lane's floor has no free lunch — three options, each a real trade:

| Option | Idle floor of the high lane | When high lane arrives | Cost |
|---|---|---|---|
| **(a) reserve floors at admission** — cap low borrowing to `total − Σ(other floors)` | **wasted** | instant, hard | burns idle capacity |
| **(b) borrow-all + preempt** — cancel a borrower on arrival | used | instant, hard | cancel + re-run wasted work |
| **(c) borrow-all + wait for natural release** | used | delayed ≈ one shard duration | none |

For **short** shard work, (c) is the sweet spot — a sub-shard-duration wait, zero wasted work. (b) earns its keep for
**long** borrowers (big ES|QL pipelines, long aggregations, scrolls), where waiting could be tens of seconds. We
implement **(b) with the low lane's floor as a guaranteed minimum**, gated by age (§7.5) so short work falls back to (c).

### 7.3 Preemption mechanism — cancel the shard, not the query
When a higher lane is below its floor and can't be served otherwise, the pool calls a borrowed reservation's reclaim hook.
For search that hook **interrupts only that one borrowing shard** — the unit holding the reclaimed slot — never the whole
query:

- **local `_search`** (`SearchService.runAsync`): the hook cancels the shard's *per-shard* `SearchShardTask`, so the
  running Lucene search stops at its next cancellation check (`lowLevelCancellation`) and releases its slot. Cancelling
  the per-shard task leaves the coordinator task and the query's other shards untouched.
- **lease / coordinator-on `_search`** (`SearchAdmissionService.reserveLocally`): with distributed admission on, the
  *lease* holds the node's slots and the covered shards skip the local acquire — so the reclaim hook cancels the lease's
  covered shards (`cancelCoveredShards`, parent task == lease id) and releases the lease, freeing its slots.
- **ES|QL data node** (`SearchService.admitSearchWork` via `preemptionHook(task)`): the hook cancels the data-node task,
  aborting that node's compute for the query (ES|QL has no finer per-batch task).

The pool **never reclaims a lane below its floor**, so the low (unboosted) lane always keeps its floor — at least one slot
— and therefore always makes progress: it is *throttled*, not stopped.

### 7.4 Preempted work still finishes — retryable, not terminal
A raw task-cancel is terminal (`TaskCancelledException`) and would *fail* the preempted query. Instead, preemption is made
**retryable** so the query finishes, just later. The signal is the **cancel reason**: a task cancelled for reclaim carries
`"preempted"` in `getReasonCancelled()` (and in the `TaskCancelledException` message, which survives the wire).

- **`_search`** (`SearchService.preemptedRetryable`): in both the local and lease-covered branches of `runAsync`, a
  preemption cancellation is translated into a retryable `ResourceRejectedException` — the *same* rejection a queue-full
  admission produces — so the coordinator re-runs the shard (on another copy / later) instead of failing the query.
- **ES|QL**: the data node tags the failure with the `"preempted"` reason; `DataNodeRequestSender` treats it as
  **retryable, not fatal** (`isPreemptionFailure` in `trackShardLevelFailure` + `isRetryableFailure`) and re-runs the
  shards — *unless data was already streamed* (then it stays fatal, avoiding duplicate pages).

So a preempted query keeps its floor slot running **and** has its yielded shards re-run to completion — throttled, but
finished. (Limitations: `_search` retry needs another copy / ES|QL single-copy retry can re-preempt up to
`unavailable_shard_resolution_attempts` then fail; the batched node-search path cancels at node, not shard, granularity.)

### 7.5 Age-gated reclaim — don't thrash on short work
Preempting *very short* work is pure waste: cancel a near-done shard and re-run it just to free a slot. The pool records
each reservation's grant time and **won't pick it as a reclaim victim until it reaches `reclaim_min_age`**
(`search.admission_control.reclaim_min_age`, `0` = always-preempt). Short work then drains naturally (option (c)); only
longer borrowers — where waiting would actually hurt the higher lane — are preempted (option (b)). Live, with
`reclaim_min_age=500ms` and sub-100ms queries, reclaims dropped from **~54/s to 0** while boosted still won its slots on
natural release.

---

## 8. The request's journey (putting it together)

```
  client ──► ① COORDINATION ──► ② SHARD ADMISSION ──► ③ EXECUTION
              reserve every       node-local slot         per-query memory
              node, all-or-       pool · lanes · queue    budget · drivers
              nothing; accept     · reject/reclaim        (query over budget
              = guaranteed                                 fails alone)
```

1. **Coordination** — accept the query only if every node can be reserved; otherwise queue/retry, then reject.
2. **Shard admission** — each node admits the shard into a lane; if full, queue briefly, else reject (surfaced as a shard
   failure the coordinator handles).
3. **Execution** — the shard runs under its per-query memory budget; exceeding it fails *that query* alone.

---

## 9. Safety, compatibility, configuration

- **Off by default.** `shard_slots_per_thread = 0` disables the pool entirely; `query_memory = 0` disables the memory
  dimension; `coordinator.enabled = false` disables distributed admission; `lane_strategy = none` keeps lanes inert;
  `reclaim_min_age = 0` keeps the old always-preempt reclaim.
- **Composes with the breakers.** The per-query breaker layers *over* REQUEST; the parent still sees every byte, so the
  node-global total stays accurate and remains the backstop. No double counting.
- **Mixed-version safe.** Coordinator admission is gated on a transport version (`search_admission`); a node that lacks
  the reserve/release actions is never sent one.
- **Fully configurable.** Node settings under `search.admission_control.*` — `shard_slots_per_thread`, `memory_limit`,
  `query_memory`, `max_queue_length`, `acquire_timeout`, `lane_strategy` (`none`/`system`/`tier`),
  `{boosted,unboosted,system}.slot_floor_fraction`, `reclaim_min_age`, and `coordinator.{enabled,accept_timeout,
  retry_interval,max_queued_searches}` — plus the per-index `index.search.boost_tier` (`boosted`/`unboosted`).

---

## 10. Observability

- **Metrics** (`SearchAdmissionMetrics`): OTel gauges/counters — `es.search.admission.{used_slots.current,
  available_slots.current, used_memory.current, queue.size, lane_used_slots.current}` and `{rejected, timed_out,
  reclaimed}.total`.
- **REST stats** (`GET /_search_admission/stats[/{nodeId}]`): a per-node nodes-action returning the full
  `ResourcePoolStats` (slots, memory, queue, **per-lane** usage/floors/borrow/reclaim/acquires, lifetime counters), the
  held-lease count, and **coordinator** counters (admitted / rejected-at-accept / queued). This is the first slice of the
  broader query-stats surface and the data source for the live dashboard.

### Live load + telemetry harness (`.agents/loadtest/`, scratch)
A self-contained Python app (stdlib only) + a Chart.js dashboard that **drives configurable `_search`/ES|QL load** and
visualizes the resource manager reacting in real time as a **pipeline of layers** (Client → Coordination → Shard
admission → Execution), plus index stats (docs/size/shards/segments) and a granular per-node table. It generates
**schema-aware** queries (reads the mapping, picks relevant queries per field type) with a `heavy` tier for
memory-stressing aggregations.

---

## 11. Bugs the live test surfaced (and why it mattered)

Running real concurrent load against a real node found two correctness bugs that unit tests missed:

1. **Metric-name boot crash** — the OTel gauge names violated the registry's naming pattern and **aborted the node at
   boot**; the test `RecordingMeterRegistry` doesn't validate names, so it slipped through. *Fixed.*
2. **`QueryMemoryBreaker` double-release** — under concurrency the node REQUEST breaker's used-bytes went **negative**
   (fatal with assertions on): `close()` returned outstanding bytes to the parent, then a shard/driver release that
   landed *after* close (releasable ordering / shared lease breaker) subtracted them again. *Fixed* by settling exactly
   once and never touching the parent after close (in production, without assertions, this had been silent accounting
   drift slowly inflating the global breaker). *Regression test added.*

Both are the kind of failure only live concurrent load reveals — which is exactly why the harness exists.

The lane/preemption demo added two more lessons (config, not code): with the **memory dimension off** (`query_memory=0`)
heavy aggregations OOM a small heap — keep a per-query budget on; and **preemption thrashes on very short work** (you
preempt a query that would have finished anyway), which is exactly what motivated the age gate (§7.5).

---

## 12. Testing

- **Unit**: `ResourcePoolTests` (floors/borrow/reclaim/queue/timeout, **reclaim-protects-floor**, **non-reclaimable not
  preempted**, **age-gate skips young borrowers**), `QueryMemoryBreakerTests` (budget enforcement, no-double-count,
  post-close release, idempotent close), `SearchLaneResolverTests` (NORMAL_ONLY / SYSTEM_AWARE / **TIER**),
  `CoordinatorSearchAdmissionTests`, `ResourcePoolStatsSerializationTests`, `SearchAdmissionMetricsTests`.
- **Single-node / integration**: `SearchServiceAdmissionControlSingleNodeTests`, `SearchAggregationMemoryBudgetSingleNodeTests`,
  `SearchServiceLaneStrategySingleNodeTests`, `SearchAdmissionStatsSingleNodeTests`.
- **Multi-node IT**: `CoordinatorSearchAdmissionIT` (accept=guaranteed; rejected-up-front-then-admitted, no leaks),
  `CoordinatorSearchAdmissionMemoryIT` (whole-shard under-lease budget), `EsqlCoordinatorAdmissionIT`,
  `EsqlCoordinatorAdmissionRejectionIT` (a rejected node fails the query fast, not a hang), plus the existing
  `EsqlActionIT`/`EsqlAdmissionControlIT`/`EsqlQueryMemoryBudgetIT` as regression.

---

## 13. What we achieved (conclusion)

We turned search from an **unbounded, reactively-protected** workload into one with an **explicit, configurable,
distributed budget** that both engines share:

- ✅ A **two-dimensional admission pool** (slots ⟂ memory) that `_search` and ES|QL both pass through — the first time
  `_search` shard execution is actually bounded.
- ✅ **Per-query memory isolation** — a query over its budget fails alone (query-scoped 429), the node survives.
- ✅ A real **distributed contract** — the coordinator reserves on every participating node before running; an accepted
  query is guaranteed it can run, with lease coverage avoiding double counting, for both `_search` and ES|QL.
- ✅ **Execution under lease** — the whole shard (and ES|QL drivers/reduction) run under the coordinator-reserved budget.
- ✅ **Priority lanes, made live and routed** — `boosted`/`unboosted`/`system` tiers map to `HIGH`/`LOW`/`SYSTEM` at every
  admission point (local, coordinator reserve, ES|QL), with floors + borrowing.
- ✅ **Preemption that finishes** — a boosted query preempts an unboosted borrower by cancelling **only that shard** (never
  the query), down to the unboosted lane's floor, and the yielded work is **re-run as a retryable rejection** so the query
  still completes — throttled, not failed — across `_search`, the lease path, and ES|QL. An **age gate** leaves short work
  alone, so preemption doesn't thrash.
- ✅ **Observability** — per-node, per-lane, per-layer stats over REST + OTel metrics, and a live dashboard that proves
  the wiring (lanes shifting, reclaims, leases) and found real concurrency bugs.
- ✅ **Safe to ship** — off by default, composes with the breakers, mixed-version gated, fully configurable.

The result is a foundation an administrator can *manage*: set real limits per deployment, give boosted work priority that
actually preempts (without starving or killing background work), see what's running, and trust that the cluster pushes
back gracefully under load instead of failing late — and a clean base for the next steps.

### What's next
- **Preemption hardening** — back off / route elsewhere on ES|QL single-copy re-preempt (today it can re-preempt up to
  `unavailable_shard_resolution_attempts` then fail); tighten the **batched node-search path** to per-shard cancellation.
- **Distributed coverage** — failover/relocation re-reservation, and **CCS/CPS nested admission** (each remote
  cluster/project admits its own part).
- **Multi-project** — several projects sharing one node's budget (this layer is the prerequisite).
- **Query control** — a cross-node query-stats API and Tasks-API integration to inspect and act on running queries.
- **Memory follow-ups** — route the remaining `_search` paths (MultiBucketConsumer count, fetch bytes) through the
  per-query breaker; later: spill / compact / partial results at the per-query seam.
- **State-of-the-art** — light async cluster-load gossip + optimistic concurrency to place work smarter without
  synchronous round-trips.

---

*Code: `server/.../common/resource/`, `server/.../search/admission/`, `SearchService`, `TransportSearchAction`, and
`x-pack/.../esql/plugin/DataNodeComputeHandler`. Settings: `search.admission_control.*` and `index.search.boost_tier`.*
