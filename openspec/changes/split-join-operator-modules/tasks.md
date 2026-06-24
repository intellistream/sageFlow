## 1. Baseline And Call-Site Audit

- [x] 1.1 Re-run `git status --short` and confirm only this change's files are dirty before implementation.
- [x] 1.2 Audit `JoinOperator` call sites and confirm which public methods and compatibility paths are still used.
- [x] 1.3 Capture the current `JoinOperator` function/member map and identify code blocks to move without behavior edits.
- [x] 1.4 Run or confirm availability of a narrow baseline test set before refactoring.

## 2. Component Skeletons

- [x] 2.1 Add internal component headers/sources for `JoinOperatorInitializer`, `JoinWindowStateExecutor`, `JoinResultEmitter`, `VSJoinRouter`, `VSJoinGlobalIndexRebuilder`, and `JoinPartitionerFactory`.
- [x] 2.2 Wire the new source files into CMake without changing runtime behavior.
- [x] 2.3 Keep component constructors explicit about owned and non-owned dependencies.

## 3. Strategy Initialization Split

- [x] 3.1 Move strategy validation, ClusteredJoin parallelism fixup, IVF dynamic parameter calculation, batch delete threshold calculation, and eviction multiplier calculation into `JoinOperatorInitializer`.
- [x] 3.2 Move JoinMethod-specific dependency wiring out of `JoinOperator::initializeWithStrategyConfig`.
- [x] 3.3 Return a runtime component bundle containing JoinMethod, WindowStates, index ids, VSJoin ids, index flags, and derived settings.
- [x] 3.4 Update `JoinOperator::open(context)` to use the initializer while preserving `std::call_once` initialization semantics.

## 4. Partitioner Factory Split

- [x] 4.1 Move ClusteredJoin, S3J, and VSJoin partitioner construction logic into `JoinPartitionerFactory`.
- [x] 4.2 Keep `JoinOperator::getPreferredPartitioner()` as a thin delegation layer for API compatibility.
- [x] 4.3 Preserve VSJoin's current temporary CentroidPartitioner-based multicast behavior.

## 5. VSJoin Routing Split

- [x] 5.1 Move target-subtask calculation from `JoinOperator` into `VSJoinRouter`.
- [x] 5.2 Move VSJoin routing debug histogram state into a route stats helper owned outside `JoinOperator`.
- [x] 5.3 Move VSJoin subtask input debug stats out of `JoinOperator`.
- [x] 5.4 Update `JoinOperator::apply(context)` to call the router before transferring `Response::record_` ownership.

## 6. VSJoin Global Rebuilder Split

- [x] 6.1 Move background rebuild start/stop/thread loop into `VSJoinGlobalIndexRebuilder`.
- [x] 6.2 Preserve snapshot ownership during rebuild by keeping `RecordView` snapshot vectors alive until index replacement completes.
- [x] 6.3 Update `JoinOperator` destructor/open lifecycle to start and stop the rebuilder through the component.
- [x] 6.4 Confirm rebuilder access to WindowState, ConcurrencyManager, config, and index ids is safe under operator destruction.

## 7. Result Materialization Split

- [x] 7.1 Move left/right copy construction and `JoinFunction::Execute` invocation into `JoinResultEmitter` or a materializer helper.
- [x] 7.2 Move Collector emission and emit/e2e metrics into the same result component.
- [x] 7.3 Preserve output ownership as `Response{ResponseType::Record, std::unique_ptr<VectorRecord>}`.

## 8. WindowState IQ Executor Split

- [x] 8.1 Move `getCandidatesFromState`, state insertion, index insertion, safe eviction, and batched index deletion into `JoinWindowStateExecutor`.
- [x] 8.2 Move candidate time-window filtering and result materialization calls into the executor.
- [x] 8.3 Preserve non-VSJoin single-target Insert-then-Query behavior.
- [x] 8.4 Preserve VSJoin multi-target Insert-then-Query behavior for each target subtask.
- [x] 8.5 Keep metrics categories equivalent to the current implementation.

## 9. Legacy Deque Path Isolation

- [x] 9.1 Search for all uses of `process`, legacy deque members, and legacy helper functions.
- [x] 9.2 If still required, move legacy deque-window logic into `LegacyJoinDequePath`; otherwise remove unused legacy helpers and members.
- [x] 9.3 Ensure RuntimeContext `apply()` no longer includes or depends on legacy deque-window state.

## 10. JoinOperator Cleanup

- [x] 10.1 Remove moved helper declarations and implementation blocks from `JoinOperator`.
- [x] 10.2 Remove no-longer-needed includes from `join_operator.h` and `join_operator.cpp`.
- [x] 10.3 Keep `JoinOperator` public API and slot/runtime validation behavior intact.
- [x] 10.4 Reduce `join_operator.cpp` to orchestration logic plus compatibility wrappers.

## 11. Tests And Verification

- [x] 11.1 Build changed C++ targets with CMake.
- [x] 11.2 Run `./build/bin/test_join_strategy_factory`.
- [x] 11.3 Run `./build/bin/test_join_operator_strategy`.
- [x] 11.4 Run relevant VSJoin tests: factory, method, operator path, routing, rebuild, load balancing, partition assignment, and load monitor as applicable to changed files.
- [x] 11.5 Run a small integration gate covering at least bruteforce and VSJoin; include IVF only if the known independent `ivf_small_batch` issue is not part of the selected filter.
- [x] 11.6 Report any skipped validation with the exact blocker and residual risk.
