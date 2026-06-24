## Context

`JoinOperator` currently combines the supported RuntimeContext `apply()` path with several unrelated implementation concerns:

- WindowState-based Insert-then-Query execution.
- Legacy deque-window update/query/validation code.
- Strategy config validation and runtime wiring.
- Algorithm-specific method initialization through `dynamic_cast`.
- VSJoin local/global index id ownership.
- VSJoin routing, multicast target selection, debug histograms, and subtask stats.
- VSJoin background global-index rebuild thread lifecycle.
- Result materialization, join-function invocation, Collector emission, and metrics.
- Preferred partitioner construction for ClusteredJoin, S3J, and VSJoin.

The refactor must keep SageFlow's current runtime boundary intact: upstream/downstream transport still uses `Response` with `std::unique_ptr<VectorRecord>`, while Join internal state/index snapshots use `RecordView = std::shared_ptr<const VectorRecord>`.

## Goals / Non-Goals

**Goals:**

- Reduce `JoinOperator` to a thin orchestration layer for lifecycle, slot selection, RuntimeContext validation, and per-record dispatch.
- Move VSJoin-specific routing and rebuild lifecycle into focused internal components.
- Move strategy initialization and method/window/index wiring out of the hot-path operator implementation.
- Move WindowState IQ execution into a component with an explicit Insert-then-Query contract.
- Move output materialization/emission into a component so candidate retrieval and result construction are separate concerns.
- Isolate legacy deque-window code so it can be tested, deprecated, or removed independently.
- Preserve correctness for shared state, partitioned state, clustered join, VSJoin multicast, index insertion/deletion, and eviction.

**Non-Goals:**

- Do not change JoinAlgorithm behavior, thresholds, partitioning semantics, or recall contracts.
- Do not replace `ConcurrencyManager`, `WindowState`, `JoinStrategyFactory`, or existing JoinMethod implementations wholesale.
- Do not change the public `Operator` API or stream transport ownership model unless a minimal adapter is unavoidable.
- Do not make IVF recall or VSJoin routing quality claims beyond what tests measure.
- Do not combine this refactor with performance algorithm changes such as new indexing policies or dedup algorithms.

## Decisions

### Decision 1: Keep `JoinOperator` as the owner-facing orchestration class

`JoinOperator` remains the public operator type used by planner and stream APIs. The refactor adds internal components rather than replacing the operator class.

Rationale: This avoids API churn and keeps existing call sites stable. The operator still owns lifecycle and component composition, but delegates implementation details.

Alternative considered: Replace `JoinOperator` with separate algorithm-specific operators. Rejected because it would duplicate stream integration and make config-driven strategy selection harder.

### Decision 2: Split VSJoin background rebuild into `VSJoinGlobalIndexRebuilder`

The rebuilder owns start/stop/join lifecycle, rebuild interval, snapshot collection, UID deduplication, offline global index rebuild, and index replacement.

Rationale: Background thread lifecycle and global-index replacement are orthogonal to per-record `apply()` execution. Isolating them reduces destructor/open complexity and makes rebuild tests easier.

Alternative considered: Keep rebuild in `JoinOperator` and only move helper functions. Rejected because thread ownership would still keep VSJoin internals in the operator header.

### Decision 3: Split VSJoin routing into `VSJoinRouter` and route diagnostics

The router computes target subtasks from `Response`, `RuntimeContext`, strategy config, and a partitioner provider. Diagnostics are held in a separate stats helper instead of static state inside `JoinOperator`.

Rationale: VSJoin routing is algorithm-specific and currently mixes routing, multicast, fallback, environment flags, and histograms.

Alternative considered: Put routing in `VSJoinMethod`. Rejected for now because routing affects where state/index insertion happens before `ExecuteEager`; the operator still needs target subtasks for IQ dispatch.

### Decision 4: Split strategy wiring into `JoinOperatorInitializer`

The initializer validates config, applies runtime-derived fixes, creates factory components, wires method-specific dependencies, computes batch delete thresholds, applies eviction multiplier, and returns a runtime component bundle.

Rationale: Initialization has a different change cadence from hot-path execution and currently contains most algorithm-specific `dynamic_cast` wiring.

Alternative considered: Expand `JoinStrategyFactory` to perform all wiring immediately. Deferred because some wiring depends on `JoinOperator` runtime ownership and should be migrated incrementally.

### Decision 5: Split WindowState IQ execution into `JoinWindowStateExecutor`

The executor performs the per-target-subtask sequence:

1. Add record to current WindowState.
2. Insert into the correct index/local index.
3. Update max-seen timestamp and compute safe eviction timestamp.
4. Evict expired records from WindowState.
5. Flush expired UIDs to `ConcurrencyManager` when the batch threshold is reached.
6. Query opposite side through `JoinMethod::ExecuteEager`.
7. Filter by event-time window.
8. Invoke result materialization for valid candidates.

Rationale: This is the Join hot path. Giving it an explicit component boundary makes concurrency and lifetime review tractable.

Alternative considered: Split insert, query, evict, and materialize into four independent services immediately. Rejected for the first pass because IQ ordering must stay easy to audit.

### Decision 6: Split result construction into `JoinResultEmitter`

The emitter/materializer creates left/right `Response` inputs for `JoinFunction::Execute`, stores result records in a local return pool or emits directly, and records emit/join-function metrics.

Rationale: Result construction is a major cost center and currently mixed with candidate iteration. It should be independently optimizable without changing candidate retrieval.

Alternative considered: Emit directly from `JoinWindowStateExecutor`. Allowed as an implementation detail only if the emitter remains a separate dependency; the executor must not own Collector policy.

### Decision 7: Isolate legacy deque-window path before deletion

Legacy functions and members are moved into `LegacyJoinDequePath` or removed after proving no supported RuntimeContext path depends on them.

Rationale: The legacy path carries old locking, trigger, and ownership assumptions that conflict with the WindowState path. Removing it without proof risks breaking backward-compatible tests.

Alternative considered: Delete immediately. Rejected until call sites and tests confirm the path is unused or intentionally unsupported.

### Decision 8: Move partitioner construction to `JoinPartitionerFactory`

`JoinOperator::getPreferredPartitioner()` delegates construction to a factory that knows ClusteredJoin, S3J, and VSJoin partitioner config.

Rationale: Partitioner construction is strategy logic, not operator execution logic. This also isolates the current VSJoin temporary use of `CentroidPartitioner`.

Alternative considered: Keep as a public override with inline switch logic. Rejected because new algorithms would continue growing `JoinOperator`.

## Risks / Trade-offs

- Hot-path behavior drift -> Preserve IQ ordering in tests and use focused integration runs for shared and partitioned strategies.
- Lifetime bugs from component references -> Components must not outlive the owning operator state; pass non-owning pointers only where lifetime is controlled by the operator and document it in constructors.
- VSJoin duplicate or missed inserts after router split -> Add routing/rebuild tests and compare target-subtask sets before and after extraction.
- Hidden legacy path dependency -> Search call sites and keep a compatibility wrapper until tests prove removal is safe.
- More files and classes -> Keep components internal and scoped; avoid new abstract interfaces unless they reduce testing or dependency complexity.
- `dynamic_cast` wiring remains during migration -> Move casts into initializer first, then consider method-level virtual initialization hooks in a later change.

## Migration Plan

1. Add new components with behavior copied from `JoinOperator` and no semantic changes.
2. Redirect `JoinOperator` to delegate one concern at a time.
3. After each concern split, run the narrow matching test target before proceeding.
4. Keep old helper functions temporarily only when useful for diff review; remove them before final completion.
5. Validate the final operator still supports both constructors, `open()`, `open(context)`, `apply(record, slot, collector)`, and `apply(record, slot, collector, context)`.
6. Rollback strategy: since this is an internal refactor, rollback is restoring delegation call sites to the original helper functions or reverting the component extraction commit.
