## Why

SageFlow currently has two partially overlapping Join test paths: `test_join_baseline_integration` is script-driven and correctness-oriented, while `test_join_datasource_modes` owns datasource/performance scenarios but is a large hard-coded test binary with its own config/reporting path. This split has already caused configuration drift, such as ground truth using one `similarity_alpha` while the runtime operator used another, and makes algorithm-change admission inconsistent.

## What Changes

- Introduce a unified Join gate runner model around `scripts/run_integration_test.py`, with explicit suites for correctness, datasource/performance, VSJoin, profiling, and combined admission.
- Refactor datasource-mode test utilities out of `test/Performance/test_join_datasource_modes.cpp` into cohesive `test/test_utils/datasource_modes/` components.
- Add datasource-mode configuration filtering and temporary TOML generation to the runner, analogous to the existing integration-test filtering path.
- Standardize result artifacts across correctness and datasource suites, including per-run directories, effective config, binary logs, JSON summaries, TSV metrics, and optional profile output.
- Make datasource testing support SIFT/dataset sampling modes (`sequential`, `random`, `stride`) through reusable config and execution utilities.
- Ensure ground truth and runtime Join operators always share the same similarity threshold, alpha, mode, window, and data split configuration.
- Add admission presets for quick correctness, VSJoin-focused validation, datasource smoke, and performance smoke.
- No runtime Join algorithm semantics should change; this is a test/tooling and validation workflow change.

## Capabilities

### New Capabilities
- `join-test-gate`: Unified command-line admission gate for Join algorithm changes, covering suite selection, build target selection, config filtering, execution, and consolidated reporting.
- `join-datasource-testing`: Reusable datasource-mode test capability covering generated data, direct dataset loading, dataset sampling, split modes, ground truth, metrics, and reporting.

### Modified Capabilities
<!-- No existing root-level capabilities are present under openspec/specs/. -->

## Impact

- `scripts/run_integration_test.py` and follow-up helper modules under a script package such as `scripts/sageflow_join_gate/`.
- `test/Performance/test_join_datasource_modes.cpp`, which should shrink to a parameterized test entrypoint after moving helpers out.
- New reusable C++ test utilities under `test/test_utils/datasource_modes/`.
- `test/IntegrationTest/join_baseline_integration_test.cpp` and `test/test_utils/integration_test_config.*` may be extended to share datasource sampling and similarity config semantics.
- Config files: `config/integration_test_cases.toml`, `config/perf_join_datasource_modes.toml`, and temporary filtered configs generated under per-run output directories.
- Test reports under `test/result/integration/`, `test/result/datasource_modes/`, and `build/metrics/` should be normalized enough for a single gate summary.
