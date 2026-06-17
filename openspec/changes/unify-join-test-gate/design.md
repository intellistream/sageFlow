## Context

SageFlow Join validation currently uses two major paths that overlap but do not share enough infrastructure:

- `scripts/run_integration_test.py` filters `config/integration_test_cases.toml`, launches `build/bin/test_join_baseline_integration`, and produces per-run integration reports.
- `test/Performance/test_join_datasource_modes.cpp` reads `config/perf_join_datasource_modes.toml` directly, owns generated/direct-load/direct-use datasource modes, writes datasource result TSV/JSON/metrics, and now supports dataset sampling.

Both paths compute ground truth, build Join pipelines, collect metrics, and write reports. Because they evolved separately, test semantics can drift. One recent example was datasource-mode ground truth using `similarity_alpha=0.001` while the runtime `bruteforce` operator still used default `alpha=0.1`, causing apparent recall loss unrelated to windowing or eviction.

Several files are also oversized:

- `test/Performance/test_join_datasource_modes.cpp`: about 1.2k LOC and mixes config parsing, datasource loading, sampling, split, ground truth, pipeline execution, result dumping, and parameterization.
- `test/IntegrationTest/join_baseline_integration_test.cpp`: about 1.2k LOC and mixes dataset/generator setup, pipeline execution, metrics, reporting, and multiple suites.
- `scripts/run_integration_test.py`: about 900 LOC and mixes CLI, config filtering, build, execution, result collection, and visualization.

The new tooling must respect SageFlow's runtime boundary: it is only test/tooling infrastructure for Join algorithm admission and must not mix demo UI, LLM provider, or paper-only claims into runtime code.

## Goals / Non-Goals

**Goals:**
- Make `scripts/run_integration_test.py` the unified entrypoint for Join algorithm change admission.
- Support explicit suites: correctness, datasource, VSJoin, performance smoke, profiling, and combined gates.
- Keep correctness and datasource/performance binaries separate initially, but drive both through the same runner and report model.
- Move datasource-mode helper logic into cohesive C++ test utilities with single responsibilities.
- Ensure runtime Join operators and ground truth always share threshold, alpha, similarity mode, window, split, and datasource sampling semantics.
- Support SIFT/dataset random sampling and stride sampling through both direct datasource tests and future integration config.
- Produce a consolidated, per-run gate report that makes pass/fail, recall, precision, duplicates, throughput, breakdown, and profile artifacts explicit.

**Non-Goals:**
- Do not change Join algorithm semantics, index implementations, partitioners, or WindowState behavior.
- Do not claim VSJoin production stability beyond what the tests actually execute.
- Do not make `test_join_datasource_modes` the only admission binary; correctness integration remains a first-class gate.
- Do not require full performance matrices for every PR; quick and full presets must remain separate.
- Do not add external dependencies unless existing Python/CMake tooling cannot satisfy the runner/report requirements.

## Decisions

### Decision 1: Use one runner with multiple suites, not one monolithic test binary

`scripts/run_integration_test.py` will become a compatibility wrapper around a small runner package, for example `scripts/sageflow_join_gate/`. The runner will expose `--suite correctness|datasource|vsjoin|perf|all`.

Rationale:
- The correctness binary and datasource/performance binary have different responsibilities and output formats today.
- Keeping binaries separate avoids risky test rewrites while still unifying the operator workflow.
- The runner can normalize build, config filtering, logs, and final reporting.

Alternative considered: merge datasource modes into `test_join_baseline_integration`. Rejected for the first phase because it would increase an already oversized file and blur correctness/performance gates before common utilities exist.

### Decision 2: Add suite registry metadata

The runner will use an internal suite registry:

| Suite | Binary | Default config | Build target | Primary purpose |
| --- | --- | --- | --- | --- |
| correctness | `build/bin/test_join_baseline_integration` | `config/integration_test_cases.toml` | `test_join_baseline_integration` | ground-truth correctness and per-method admission |
| datasource | `build/bin/test_join_datasource_modes` | `config/perf_join_datasource_modes.toml` | `test_join_datasource_modes` | datasource modes, direct SIFT/JSON/random, sampling, split behavior |
| vsjoin | existing VSJoin unit binaries plus selected correctness/datasource cases | mixed | VSJoin targets | VSJoin-specific correctness and control-plane checks |
| perf | `test_join_datasource_modes` plus optional profile | perf config | `test_join_datasource_modes` | small controlled performance and hotspot checks |

Rationale:
- Explicit metadata prevents hard-coded assumptions such as one config file or one binary path.
- The build step can build only the required targets, avoiding slow or fragile full builds.

### Decision 3: Generate effective configs per suite

The runner will continue generating per-run filtered TOML for correctness tests, and will add equivalent generation for datasource/performance tests. The effective config will live in `<output-dir>/run_<timestamp>/filtered_config.toml` or `<suite>_filtered_config.toml`, and the binary will receive it through `SAGEFLOW_TEST_CONFIG_PATH`.

Rationale:
- Tests must be reproducible from the effective config.
- The runner can safely apply CLI overrides without mutating checked-in config files.
- This requires `test_join_datasource_modes` to read `SAGEFLOW_TEST_CONFIG_PATH` instead of only `config/perf_join_datasource_modes.toml`.

### Decision 4: Extract datasource-mode C++ helpers by responsibility

Move code out of `test_join_datasource_modes.cpp` into `test/test_utils/datasource_modes/`:

- `config`: parse and validate datasource-mode TOML.
- `dataset_sampling`: build deterministic `sequential`, `random`, and `stride` dataset index lists.
- `record_loader`: implement `generate_save_load`, `direct_load`, and `generate_direct_use`.
- `splitter`: implement `duplicate`, `half_split`, and `interleaved`.
- `ground_truth`: compute expected pairs and manage safe ground-truth caching.
- `pipeline_runner`: build and execute stream pipelines and inject full `JoinStrategyConfig`.
- `result_writer`: write sink JSON, TSV reports, and metrics paths.

Rationale:
- Each utility can have targeted tests and remain below the project's size ceiling.
- Ground-truth semantics can be shared with integration tests later.
- The top-level parameterized test becomes an orchestration wrapper instead of a logic dump.

### Decision 5: Treat similarity config as a typed test contract

Every test path that computes expected matches and executes runtime Join MUST pass one shared similarity contract into both sides:

- threshold
- similarity mode
- alpha
- window size
- step/trigger interval
- split mode
- sample mode and seed if dataset-backed

Rationale:
- This prevents the `alpha=0.001` expected vs `alpha=0.1` runtime regression from recurring.
- It also makes approximate-index failures distinguishable from configuration mismatch.

### Decision 6: Normalize reports but preserve detailed artifacts

Each suite will continue writing its native artifacts, but the runner will write a consolidated `gate_report.json` with:

- suite name, binary, effective config, command, return code
- case counts, pass/fail counts, skipped/disabled counts
- per-algorithm recall, precision, F1, throughput
- duplicate counts or duplicate ratio when available
- JoinMetrics breakdown: candidate fetch, similarity, join function, emit, index, window, lock wait
- profile artifacts if enabled
- explicit failure classification: correctness failure, approximate recall threshold failure, timeout, build failure, disabled/no-test warning

Rationale:
- CI and humans need one summary, while researchers still need detailed TSV/JSON logs.

### Decision 7: Keep presets small and explicit

Define named presets rather than making `all` the default:

- `quick`: core Join unit tests plus small correctness matrix.
- `vsjoin`: VSJoin unit targets plus small VSJoin correctness/datasource cases.
- `datasource-smoke`: one generated case and one direct-load SIFT random-sampling case.
- `perf-smoke`: small p sweep with fixed size/window, no full matrix.
- `full`: opt-in larger matrix.

Rationale:
- The project has high-output workloads that can generate millions of pairs; accidental full matrices can waste time or memory.

## Risks / Trade-offs

- [Risk] Refactoring large tests may accidentally change test semantics → Mitigation: extract one component at a time and run existing smoke tests after each extraction.
- [Risk] Datasource and integration configs diverge further during migration → Mitigation: add a shared config model for similarity, split, and sampling first, then migrate call sites.
- [Risk] Existing report consumers may expect old paths → Mitigation: keep old artifacts while adding `gate_report.json`; do not remove legacy TSV/JSON initially.
- [Risk] `test_join_datasource_modes` config path behavior changes → Mitigation: default to current file when `SAGEFLOW_TEST_CONFIG_PATH` is unset.
- [Risk] Approximate indexes such as IVF can fail strict recall thresholds → Mitigation: classify approximate recall threshold failures separately from pipeline/runtime failures.
- [Risk] VSJoin disabled integration tests can be mistaken for coverage → Mitigation: runner MUST report enabled/disabled/no-test status explicitly.

## Migration Plan

1. Add `SAGEFLOW_TEST_CONFIG_PATH` support to `test_join_datasource_modes` while preserving the current default config.
2. Extend `run_integration_test.py` with `--suite datasource` and suite metadata for datasource binary/config/target.
3. Add datasource temporary config generation with CLI overrides for mode, datasource type/path, sampling, split, alpha, window, size, and parallelism.
4. Add `gate_report.json` output that merges correctness and datasource reports.
5. Extract C++ datasource-mode helpers one responsibility at a time, with focused unit tests for sampling, split, and ground truth.
6. Extend integration dataset config to support the same sampling and similarity contract.
7. Add presets for quick, VSJoin, datasource-smoke, perf-smoke, and full.
8. Update documentation and examples only after behavior is implemented and verified.

Rollback is straightforward for early phases: keep old binary defaults and do not remove legacy reports. If the runner fails, users can still call the test binaries directly.

## Open Questions

- Should `run_integration_test.py` keep its name for compatibility, or should a new `scripts/run_join_gate.py` become the primary command with the old script as a wrapper?
- Should datasource-mode reports use the same `TestReportGenerator` C++ structures as integration tests, or should normalization happen only in Python initially?
- Should `perf` gates enforce thresholds by default, or only collect metrics unless explicit baseline files are provided?
- How should approximate algorithms declare expected recall bands per dataset and split mode?
