## ADDED Requirements

### Requirement: Unified Join Gate Suites
The system SHALL provide a unified Join test gate entrypoint that can run named suites for correctness, datasource validation, VSJoin validation, performance smoke, and combined admission.

#### Scenario: Run correctness suite
- **WHEN** a user invokes the gate runner with the correctness suite and selected methods, data sizes, and parallelism
- **THEN** the runner SHALL build or use the correctness binary, generate an effective integration config, execute the selected cases, and return non-zero if enabled correctness cases fail.

#### Scenario: Run datasource suite
- **WHEN** a user invokes the gate runner with the datasource suite and datasource options
- **THEN** the runner SHALL build or use the datasource binary, generate an effective datasource config, execute the selected datasource cases, and collect datasource reports under the run directory.

#### Scenario: Run VSJoin suite
- **WHEN** a user invokes the gate runner with the VSJoin suite
- **THEN** the runner SHALL execute VSJoin unit/control-plane targets and selected VSJoin end-to-end cases, and it SHALL explicitly report disabled or zero-test VSJoin integration binaries as insufficient coverage rather than success evidence.

### Requirement: Per-Run Effective Configuration
The system SHALL write the exact effective configuration used by each suite into the per-run output directory and SHALL pass that configuration to the target binary without mutating checked-in config files.

#### Scenario: Filter correctness matrix
- **WHEN** a user passes method, data-size, parallelism, window, or gtest filters for correctness testing
- **THEN** the runner SHALL write a filtered TOML in the run directory and set `SAGEFLOW_TEST_CONFIG_PATH` to that file for `test_join_baseline_integration`.

#### Scenario: Filter datasource matrix
- **WHEN** a user passes datasource mode, datasource path, split mode, sample mode, alpha, data-size, or parallelism filters for datasource testing
- **THEN** the runner SHALL write a filtered datasource TOML in the run directory and set `SAGEFLOW_TEST_CONFIG_PATH` to that file for `test_join_datasource_modes`.

### Requirement: Targeted Build Execution
The system SHALL build only the required CMake targets for the selected suite unless the user explicitly requests a full build.

#### Scenario: Build datasource suite
- **WHEN** the user runs the datasource suite with build enabled
- **THEN** the runner SHALL build `test_join_datasource_modes` and its dependencies, not every test binary in the repository.

#### Scenario: Build VSJoin suite
- **WHEN** the user runs the VSJoin suite with build enabled
- **THEN** the runner SHALL build the configured VSJoin unit targets and any selected end-to-end binary.

### Requirement: Consolidated Gate Report
The system SHALL create one consolidated gate report per run that records suite outcomes, executed commands, effective configs, result artifacts, recall, precision, throughput, duplicates when available, JoinMetrics breakdown, and failure classifications.

#### Scenario: Mixed pass and approximate recall failure
- **WHEN** correctness cases pass but an approximate-index datasource case fails a recall threshold
- **THEN** the report SHALL distinguish the approximate recall threshold failure from a pipeline crash, build failure, or exact-method correctness failure.

#### Scenario: Profile output enabled
- **WHEN** profiling is enabled for a selected case
- **THEN** the report SHALL include the profile file path and summarized top hotspots if pprof output was generated.

### Requirement: Admission Presets
The system SHALL provide explicit presets for quick, VSJoin, datasource-smoke, perf-smoke, and full gates, and SHALL NOT run the full matrix by default.

#### Scenario: Quick preset
- **WHEN** a user runs the quick preset
- **THEN** the runner SHALL execute a bounded set of core Join unit tests and a small correctness matrix suitable for PR admission.

#### Scenario: Full preset
- **WHEN** a user runs the full preset
- **THEN** the runner SHALL make the expanded matrix explicit in the dry-run or effective config before executing it.

### Requirement: Backward-Compatible Script Entry
The existing script path `scripts/run_integration_test.py` SHALL remain usable for existing correctness workflows while delegating new functionality to a smaller internal runner structure.

#### Scenario: Existing command compatibility
- **WHEN** a user runs `scripts/run_integration_test.py --methods bruteforce ivf --parallelism 1 2 --data-sizes 500`
- **THEN** the behavior SHALL remain compatible with the current correctness integration flow unless new suite options are explicitly supplied.
