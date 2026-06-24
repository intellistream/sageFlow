## Why

`JoinOperator` has grown into a monolithic runtime component that mixes operator orchestration, WindowState IQ execution, index lifecycle wiring, VSJoin routing, VSJoin background rebuild, result emission, and legacy deque-window compatibility code. This makes Join correctness and performance work risky because changes to one concern require editing a 1400+ line implementation file with multiple overlapping concurrency and lifecycle models.

## What Changes

- Split VSJoin background global-index rebuild lifecycle out of `JoinOperator`.
- Split VSJoin routing, multicast target calculation, and debug routing statistics out of `JoinOperator`.
- Split strategy-config initialization and method/window/index wiring out of `JoinOperator`.
- Split WindowState IQ insert/query/evict execution into a dedicated executor while preserving per-record Insert-then-Query semantics.
- Split result materialization and Collector emission from candidate retrieval and verification.
- Isolate or remove the legacy deque-window path after proving it is not part of the supported RuntimeContext `apply()` path.
- Move preferred partitioner construction into a strategy/factory component instead of keeping algorithm-specific construction logic in `JoinOperator`.
- Preserve public operator behavior, JoinAlgorithm semantics, `Response` ownership boundaries, `RecordView` window/index lifetime semantics, and existing test entry points.

## Capabilities

### New Capabilities

- `join-operator-modularization`: Defines the modular boundaries, runtime contracts, and verification requirements for splitting `JoinOperator` into focused internal components.

### Modified Capabilities

None.

## Impact

- Affected runtime code:
  - `include/operator/join_operator.h`
  - `src/operator/join_operator.cpp`
  - new internal headers/sources under `include/operator/` and `src/operator/`
  - VSJoin support code under `include/operator/join_operator_methods/vsjoin_components/`
  - Join strategy factory and partitioner utility code where construction logic is moved
- Affected behavior:
  - No intentional external behavior change.
  - No change to operator public API unless required for dependency injection of internal components.
  - No change to `Response` transport ownership or `RecordView` storage/index lifetime model.
- Required verification:
  - Join strategy/factory tests.
  - Join operator strategy tests.
  - Join integration pipeline tests for brute force, shared-index methods, clustered join, and VSJoin.
  - VSJoin routing/rebuild/load-balancing tests for VSJoin-specific splits.
