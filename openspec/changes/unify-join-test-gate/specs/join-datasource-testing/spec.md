## ADDED Requirements

### Requirement: Datasource Mode Configuration
The system SHALL support datasource-mode test configuration for generated data, generated-save-load data, direct dataset loading, dataset sampling, split mode, similarity contract, and Join method matrix expansion.

#### Scenario: Direct SIFT random sample
- **WHEN** a datasource-mode test config specifies `mode=direct_load`, `data_source.type=dataset`, `sample_mode=random`, a `sample_seed`, and a SIFT `.fvecs` path
- **THEN** the test SHALL select a deterministic random subset of dataset vectors for the requested size without reading only the first N vectors.

#### Scenario: Default sequential compatibility
- **WHEN** a datasource-mode test config omits sampling fields
- **THEN** the test SHALL preserve the current sequential direct-load behavior.

### Requirement: Dataset Sampling Semantics
The system SHALL define dataset sampling modes `sequential`, `random`, and `stride`, with explicit seed, offset, stride, and loop semantics.

#### Scenario: Sequential sampling
- **WHEN** `sample_mode=sequential` and `sample_offset=K`
- **THEN** the test SHALL select dataset indices starting at K in order until the requested count is reached or the dataset is exhausted.

#### Scenario: Random sampling
- **WHEN** `sample_mode=random` and `sample_seed=S`
- **THEN** the test SHALL shuffle dataset indices deterministically with seed S and select from that shuffled order.

#### Scenario: Stride sampling
- **WHEN** `sample_mode=stride`, `sample_offset=K`, and `sample_stride=M`
- **THEN** the test SHALL select indices K, K+M, K+2M, and so on, respecting loop configuration.

#### Scenario: Unknown sampling mode
- **WHEN** the config specifies an unknown sample mode
- **THEN** the test SHALL fail early with a clear configuration error.

### Requirement: Shared Similarity Contract
The datasource-mode test SHALL use one shared similarity contract for ground-truth computation and runtime Join operator construction.

#### Scenario: SIFT alpha consistency
- **WHEN** a datasource-mode SIFT test config specifies `similarity_alpha=0.001`
- **THEN** ground truth and the runtime Join method SHALL both use alpha `0.001`.

#### Scenario: Exact brute-force recall
- **WHEN** a brute-force datasource-mode test uses a deterministic dataset sample and compares against ground truth computed with the same similarity contract
- **THEN** recall SHALL be 1.0 unless the test explicitly configures a non-exact algorithm or known approximation.

### Requirement: Ground Truth Cache Safety
The datasource-mode test SHALL only use cached dataset ground truth when the cache key fully matches the sampled data semantics, or it SHALL disable cache usage for sampling modes not represented in the cache key.

#### Scenario: Random sample avoids stale cache
- **WHEN** a direct-load dataset test uses `sample_mode=random`
- **THEN** the test SHALL NOT reuse a cached ground-truth entry created for the first N sequential dataset vectors.

#### Scenario: Sequential default cache remains valid
- **WHEN** a direct-load dataset test uses default sequential sampling from offset 0 with stride 1
- **THEN** the test MAY use existing dataset ground-truth cache entries if all other key fields match.

### Requirement: Reusable Datasource Test Utilities
Datasource-mode helper logic SHALL be split into reusable, single-responsibility C++ test utilities rather than remaining in one monolithic performance test file.

#### Scenario: Sampling unit test
- **WHEN** dataset sampling helper code is changed
- **THEN** unit tests SHALL verify sequential, random, stride, offset, loop, and exhausted-dataset behavior without launching a full Join pipeline.

#### Scenario: Split unit test
- **WHEN** stream split helper code is changed
- **THEN** unit tests SHALL verify duplicate, half-split, and interleaved UID/timestamp semantics without launching a full Join pipeline.

#### Scenario: Ground truth unit test
- **WHEN** ground-truth helper code is changed
- **THEN** unit tests SHALL verify threshold, alpha, similarity mode, UID modulo handling, and time-window filtering.

### Requirement: Datasource Report Compatibility
The datasource-mode binary SHALL emit machine-readable summaries that the unified gate runner can collect without parsing human-only logs.

#### Scenario: Datasource report generated
- **WHEN** datasource-mode tests complete
- **THEN** the binary SHALL write a JSON summary containing per-case method, mode, datasource type, sample mode, size, parallelism, window, recall, precision, F1, actual count, expected count, and timing metrics.

#### Scenario: Metrics TSV retained
- **WHEN** datasource-mode tests complete
- **THEN** existing TSV and JoinMetrics artifacts SHALL remain available for performance analysis.
