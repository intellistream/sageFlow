## ADDED Requirements

### Requirement: Thin JoinOperator orchestration
`JoinOperator` SHALL remain the public operator integration point while delegating algorithm-specific routing, initialization, rebuild, WindowState execution, and result emission to focused internal components.

#### Scenario: RuntimeContext apply delegates internal concerns
- **WHEN** `JoinOperator::apply(Response&&, int, Collector&, const RuntimeContext&)` processes a valid record
- **THEN** it validates context, resolves slot/state/index handles, delegates per-record execution to internal components, and preserves the existing Insert-then-Query order.

#### Scenario: Public API remains stable
- **WHEN** existing planner or stream code constructs and opens a `JoinOperator`
- **THEN** the existing constructors, `open()`, `open(context)`, `apply(...)`, `setSlots(...)`, and `getPreferredPartitioner(...)` entry points remain source-compatible unless explicitly covered by a migration note.

### Requirement: WindowState IQ executor
The system SHALL provide a WindowState IQ executor that owns the per-record state/index execution sequence for the supported RuntimeContext path.

#### Scenario: Non-VSJoin record execution
- **WHEN** a non-VSJoin record is applied with a RuntimeContext
- **THEN** the executor inserts the record into the current side state and index, updates max-seen timestamp, performs safe eviction and batched index deletion, queries the opposite side, filters candidates by event-time window, and materializes join results.

#### Scenario: VSJoin multicast execution
- **WHEN** VSJoin routing returns multiple target subtasks for one record
- **THEN** the executor applies the same Insert-then-Query sequence for each target subtask without sharing mutable per-target state outside the executor call.

#### Scenario: Shared state recall ordering
- **WHEN** two matching records arrive concurrently on opposite sides
- **THEN** the executor preserves the current IQ ordering guarantee that at least one side can observe the other after insertion, subject to the existing WindowState and ConcurrencyManager synchronization contracts.

### Requirement: VSJoin routing component
The system SHALL provide a VSJoin routing component for target-subtask calculation, multicast handling, fallback behavior, and routing diagnostics.

#### Scenario: Router computes target subtasks
- **WHEN** VSJoin processes a record with a configured partitioner
- **THEN** the router returns a deduplicated non-empty list of target subtasks bounded by the RuntimeContext parallelism.

#### Scenario: Router diagnostics remain optional
- **WHEN** VSJoin routing debug environment variables are not enabled
- **THEN** routing diagnostics do not add mandatory logging or shared-state overhead beyond the existing disabled-debug behavior.

### Requirement: VSJoin global index rebuilder
The system SHALL provide a VSJoin global index rebuilder component that owns background rebuild lifecycle and global index replacement.

#### Scenario: Rebuilder lifecycle follows operator lifecycle
- **WHEN** a VSJoin operator is opened and later destroyed
- **THEN** the rebuilder starts at most once, stops safely, joins its background thread, and does not access destroyed WindowState or ConcurrencyManager objects.

#### Scenario: Rebuilder preserves snapshot lifetime
- **WHEN** the rebuilder collects WindowState snapshots for global index rebuild
- **THEN** it keeps `RecordView` snapshot ownership alive until the replacement indexes have been built and swapped.

### Requirement: Strategy initialization component
The system SHALL provide an initializer component that validates strategy config, creates strategy components, and wires method/window/index dependencies.

#### Scenario: Initializer wires supported methods
- **WHEN** a Join strategy is opened for bruteforce, IVF, HNSW, HDR tree, LSH, ClusteredJoin, S3J, or VSJoin
- **THEN** the initializer creates or receives the correct JoinMethod, WindowState, index ids, and method-specific dependencies without placing algorithm-specific wiring in the hot-path apply implementation.

#### Scenario: Runtime-derived config remains compatible
- **WHEN** ClusteredJoin or IVF requires runtime-derived parameters such as `num_partitions`, `ivf_nlist`, `ivf_nprobes`, batch-delete threshold, or eviction multiplier
- **THEN** the initializer applies the same effective values as the current `JoinOperator` implementation unless a task explicitly updates tests and documents a behavior change.

### Requirement: Result materialization and emission component
The system SHALL provide a result materialization/emission component that invokes `JoinFunction::Execute`, constructs output `Response` objects, and emits through `Collector`.

#### Scenario: Join result ownership boundary
- **WHEN** a candidate pair produces a joined record
- **THEN** the output remains a `Response` owning `std::unique_ptr<VectorRecord>` and is emitted through the existing Collector API.

#### Scenario: Metrics remain attributable
- **WHEN** candidate verification, join function execution, and output emission occur
- **THEN** existing JoinMetrics categories continue to record candidate fetch, similarity verification, join function, emit, and end-to-end latency without double-counting intentional timer scopes.

### Requirement: Legacy deque path isolation
The system SHALL isolate the legacy deque-window path from the supported WindowState RuntimeContext path before removal or deprecation.

#### Scenario: Legacy code is not mixed with WindowState execution
- **WHEN** the RuntimeContext `apply()` path is used
- **THEN** it does not depend on `left_records_`, `right_records_`, deque-window mutexes, old trigger checks, or lock-held candidate validation helpers.

#### Scenario: Legacy removal is evidence based
- **WHEN** legacy deque-window functions or members are removed
- **THEN** call-site search and tests demonstrate that supported execution paths and compatibility tests do not require them.

### Requirement: Partitioner construction factory
The system SHALL move preferred partitioner construction for algorithm strategies into a factory or strategy helper outside the main `JoinOperator` implementation.

#### Scenario: Algorithm-specific partitioner creation is delegated
- **WHEN** ClusteredJoin, S3J, or VSJoin requests a preferred partitioner
- **THEN** `JoinOperator` delegates construction to the factory/helper and does not embed algorithm-specific construction switches in its hot-path implementation file.

### Requirement: Verification coverage
The refactor SHALL be validated with targeted unit, integration, and VSJoin tests matching the components changed.

#### Scenario: Shared and partitioned Join validation
- **WHEN** WindowState execution, initializer, emitter, or partitioner code is changed
- **THEN** the matching Join strategy/factory/operator tests and at least one small integration gate run are executed or explicitly reported as unavailable.

#### Scenario: VSJoin validation
- **WHEN** VSJoin router or rebuilder code is changed
- **THEN** VSJoin factory, method, operator path, routing, rebuild, load-balancing, partition assignment, or load-monitor tests are executed according to the touched component.
