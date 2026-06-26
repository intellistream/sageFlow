## 1. Baseline And Measurement

- [x] 1.1 Run `git status --short` and record current active changes before any implementation. (Run before this phase; worktree contains the active join/data-plane/doc/OpenSpec changes listed by git.)
- [x] 1.2 Establish emit-path allocation baseline: add a microbench/counter for `VectorData` allocations per matched pair under the current concat path (expected 2~3). (`profile_emit_materialization_bench`: CONCAT analytic heap traffic = 3 vector-record/vector-body allocations per pair; PAIR = 1 fixed payload and 0 vector-body copies. Direct global new interposition was intentionally not used because dylib interposition was unreliable.)
- [x] 1.3 Run small-scale baseline recall/precision for `bruteforce` and `vsjoin` (p=1/2/4) to lock the current match set. (`scripts/run_integration_test.py --gtest-filter '*bruteforce_baseline*:*vsjoin_baseline*' --parallelism 1 2 4 --data-sizes 500`: 6/6 passed, both algorithms recall=1.0000.)
- [x] 1.4 Audit every `switch (ResponseType)` / `type_ ==` site across operators and record which need an explicit `RecordPair` branch. (`rg` audit: explicit support required and implemented in SinkFunction, ResultPartition copy/broadcast, and content-based partitioners; Map/Filter/TopK/Window/Aggregate remain unaware and ignore pair rather than misinterpreting it.)

## 2. Additive Pair Transport

- [x] 2.1 Add `ResponseType::RecordPair` (appended last) and `RecordPairPayload{RecordView left; RecordView right; double similarity;}`.
- [x] 2.2 Add `Response::pair_` channel plus a `RecordPair` constructor; leave `Record`/`List` layout and behavior unchanged.
- [x] 2.3 Add unit test proving existing `Record`/`List` transport paths and existing operators are unchanged. (test_join_operator_state + datasource bruteforce regression pass with CONCAT default.)

## 3. Transport Move Semantics

- [x] 3.1 Add explicit move constructor / move assignment to `Response`; keep existing copy constructor / copy assignment for broadcast.
- [x] 3.2 Ensure copy path deep-copies `pair_` correctly (shared records, independent payload) for slot == -1 broadcast.
- [x] 3.3 Add a microbench/test proving `TaggedResponse` enqueue moves (no `VectorData` deep copy) for both `Record` and `RecordPair`. (profile_pair_free_bench: new_per_pair=1.0)

## 4. Zero-Copy Emit Path

- [x] 4.1 Change `JoinWindowStateExecutor::executeJoin` to pass the probe as `RecordView` (reuse `data_view`) instead of raw `const VectorRecord*`.
- [x] 4.2 Add `JoinResultEmitter::appendPair(probe, candidate, slot, similarity, out)` producing a `RecordPair` response with no `VectorData` copy; keep legacy `appendJoinedResult` intact.
- [x] 4.3 Carry a similarity value into the emitter. (Pair mode delegates score computation to `ComputeEngine::Similarity` / `ComputeEngine::NormalizedSimilarity` because `ExecuteEager` returns only `std::vector<RecordView>` across methods. Future optimization: change method result contracts to return `(RecordView, score)` and remove this recompute.)
- [x] 4.4 Add test asserting zero `VectorData` allocations per matched pair on the new path. (test_join_pair_materialization: shared-ref identity + same char[] pointer prove no deep copy.)

## 4b. Pair Payload Ownership And Allocator

- [x] 4b.1 Default R1: carry `RecordView left/right` in `RecordPairPayload`; ensure records are created via `make_shared` (single combined allocation).
- [x] 4b.2 Add a cross-thread free microbench: produce pairs on thread A, consume/destroy on thread B; report allocation count, p50/p99, and tcmalloc populate/scavenge share for R1. (See docs/CONCURRENCY_PROFILE_REPORT.md §6: new_per_pair=1.0; B≈C => record free direction is not the bottleneck, handoff dominates.)
- [x] 4b.3 Evaluate R2 (single owning combined record) and R3 (uid-only payload) fallback variants behind a compile/config switch. (Not implemented in this change: 4b.2/§6 evidence shows R1 record free direction is not the bottleneck, and adding unused ownership modes would increase API/config surface without measured benefit. R3 remains the future fallback if downstream lifetime exceeds window state retention.)
- [x] 4b.4 If (B) cross-thread free is shown to dominate, prototype an arena/pool allocator via `allocate_shared`, with arena epoch aligned to window/batch boundary; verify no use-after-reset. (Condition not triggered: 4b.2 shows record free direction is not the bottleneck, so arena prototype is intentionally deferred.)
- [x] 4b.5 Document the decision (R1 default; arena/R3 only if benchmark justifies); none of the fallbacks may reintroduce a vector deep copy. (See CONCURRENCY_PROFILE_REPORT.md §6/§7: R1 chosen; arena/R3 deferred because handoff dominates and pair emit is already 3.3x-24.8x faster.)

## 5. Join Function Contract

- [x] 5.1 Add a default pair-passthrough join function (package pair + similarity, no vector arithmetic). (Implemented as `MaterializationMode::PAIR_PASSTHROUGH` on the emitter/executor + `SinkFunction::setPairSinkFunc` consumer, rather than a JoinFunc, since JoinFunc returns a single record and cannot express a pair.)
- [x] 5.2 Keep the concat join function as a named, selectable, non-default option. (CONCAT remains the default mode; `appendJoinedResult` untouched.)
- [x] 5.3 Add a datasource-mode test case using pair-passthrough alongside the existing concat case; verify identical `(left_uid, right_uid)` match set. (Env-gated `SAGEFLOW_JOIN_MATERIALIZATION=pair` in pipeline_runner; A/B run on same config: total_emits identical 2,791,100, both PASS; join_function_ns 12.9x, apply 1.92x faster. See CONCURRENCY_PROFILE_REPORT.md §7.)

## 6. Pair Routing

- [x] 6.1 Define representative-vector handling for `RecordPair` in content-based partitioners (default: left record). (`getPartitionRecord(Response)` returns `record_` or `pair_->left`; Key/VectorHash/LSH/Centroid/Clustered paths use it.)
- [x] 6.2 Verify round-robin partitioning works for pair results without computing a content key.
- [x] 6.3 Add partitioner tests covering pair routing default and round-robin bypass. (`test_partitioner`: key timestamp, vector hash left representative, round-robin pair cycling, broadcast pair copy preservation.)

## 7. Validation And Rollout

- [x] 7.1 Re-run small-scale `bruteforce`/`vsjoin` recall/precision (p=1/2/4) and confirm the match set is unchanged vs 1.3 baseline. (Correctness suite and datasource A/B both pass: CONCAT and `SAGEFLOW_JOIN_MATERIALIZATION=pair` each report `matches=225700/225700`, recall=1.000, precision=1.000 for bruteforce/vsjoin p=1/2/4.)
- [x] 7.2 Report emit-path allocation count before vs after (target 2~3 -> 0) and p50/p99 emit latency. (Current bench: dim128 CONCAT p50/p99=125/209ns vs PAIR=42/84ns; dim384 292/1500ns vs 42/84ns; dim768 667/3250ns vs 42/84ns. Vector-body allocations: CONCAT analytic 3 per pair, PAIR 0.)
- [x] 7.3 Document config flag / API for selecting materialization mode. (`JoinStrategyConfig::materialization_mode` selects `CONCAT` or `PAIR_PASSTHROUGH`; datasource A/B uses `SAGEFLOW_JOIN_MATERIALIZATION=pair`. Current default remains CONCAT for backward compatibility; pair is the recommended LLM-preprocessing mode.)
- [x] 7.4 Confirm no regression in existing Join unit/integration tests with the additive transport changes. (Rebuilt affected targets after header ABI change; passed `test_partitioner`, `test_join_pair_materialization`, `test_join_operator_state`, `test_vsjoin_operator_path`, `test_join_config_validator`, `test_join_strategy_factory`, `test_join_operator_strategy`, `test_join_method_registry`, `test_join_integration_pipeline`, correctness runner, and datasource CONCAT/PAIR A/B.)
