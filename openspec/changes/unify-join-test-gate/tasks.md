## 1. Baseline And Compatibility

- [x] 1.1 Run `git status --short` in `sageFlow/` and record unrelated dirty files before editing.
- [x] 1.2 Capture current runner behavior with `python3 scripts/run_integration_test.py --methods bruteforce --parallelism 1 --data-sizes 500 --dry-run`.
- [x] 1.3 Build and run current smoke targets: `test_join_baseline_integration` and `test_join_datasource_modes`.
- [x] 1.4 Record current report locations and schemas for integration JSON/MD, datasource TSV/JSON, and JoinMetrics TSV.

## 2. Datasource Config Path And Sampling Contract

- [x] 2.1 Modify `test_join_datasource_modes` to read `SAGEFLOW_TEST_CONFIG_PATH`, defaulting to `config/perf_join_datasource_modes.toml`.
- [x] 2.2 Add or preserve dataset sampling config fields: `sample_mode`, `sample_seed`, `sample_offset`, `sample_stride`, and `loop`.
- [x] 2.3 Ensure non-default dataset sampling does not reuse stale ground-truth cache keyed only for sequential first-N data.
- [x] 2.4 Ensure all datasource-mode Join methods receive full `JoinStrategyConfig` so runtime and ground truth share threshold, alpha, mode, window, and split semantics.
- [x] 2.5 Verify SIFT sequential/random sampling smoke returns recall=1.0 for exact bruteforce with matching alpha.

## 3. Extract Datasource Mode Utilities

- [x] 3.1 Create `test/test_utils/datasource_modes/config.{h,cpp}` for `DataSourceModeConfig` and TOML parsing.
- [x] 3.2 Create `test/test_utils/datasource_modes/dataset_sampling.{h,cpp}` for sequential/random/stride sampling.
- [x] 3.3 Add focused unit tests for dataset sampling: sequential, random determinism, stride, offset, loop, and exhausted dataset.
- [x] 3.4 Create `test/test_utils/datasource_modes/record_loader.{h,cpp}` for `generate_save_load`, `direct_load`, and `generate_direct_use`.
- [x] 3.5 Create `test/test_utils/datasource_modes/splitter.{h,cpp}` for duplicate, half-split, and interleaved stream construction.
- [x] 3.6 Create `test/test_utils/datasource_modes/ground_truth.{h,cpp}` for L2 similarity, alpha/mode handling, UID modulo, and time-window filtering.
- [x] 3.7 Create `test/test_utils/datasource_modes/pipeline_runner.{h,cpp}` for stream pipeline construction, strategy config injection, execution, and metrics collection.
- [x] 3.8 Create `test/test_utils/datasource_modes/result_writer.{h,cpp}` for sink JSON, report TSV, metrics TSV, and future JSON summary.
- [x] 3.9 Reduce `test/Performance/test_join_datasource_modes.cpp` to parameterization and assertions only.

## 4. Standardize Datasource Reports

- [x] 4.1 Add a datasource-mode JSON summary containing method, mode, datasource type, sample mode, size, parallelism, window, recall, precision, F1, actual count, expected count, time, and breakdown fields.
- [x] 4.2 Preserve existing datasource TSV and per-case sink JSON outputs.
- [x] 4.3 Include duplicate count or duplicate ratio when sink dedup information is available.
- [x] 4.4 Verify runner can collect datasource JSON summaries without parsing human logs.

## 5. Runner Suite Registry

- [x] 5.1 Split `scripts/run_integration_test.py` into a compatibility wrapper plus internal modules under `scripts/sageflow_join_gate/`.
- [x] 5.2 Add a suite registry mapping suite names to binary path, default config, build target, result collectors, and required environment variables.
- [x] 5.3 Add `--suite correctness|datasource|vsjoin|perf|all`, defaulting to current correctness behavior for compatibility.
- [x] 5.4 Change `--build` to build only selected suite targets by default.
- [x] 5.5 Add `--full-build` or equivalent opt-in for current broad build behavior.

## 6. Datasource Suite CLI And Config Generation

- [x] 6.1 Add CLI flags for datasource suite: `--mode`, `--data-source-type`, `--data-source-file`, `--expected-dim`, and `--split-mode`.
- [x] 6.2 Add CLI flags for sampling: `--sample-mode`, `--sample-seed`, `--sample-offset`, `--sample-stride`.
- [x] 6.3 Add CLI flags for similarity/window overrides: `--similarity-alpha`, `--similarity-mode`, `--window-time-ms`, `--time-interval-ms`.
- [x] 6.4 Generate datasource filtered TOML in the per-run directory without modifying checked-in configs.
- [x] 6.5 Verify `--suite datasource` can run a SIFT random-sampling smoke case from only CLI flags.

## 7. Consolidated Gate Reporting

- [x] 7.1 Implement `gate_report.json` with suite results, command lines, effective config paths, return codes, artifacts, and failure classification.
- [x] 7.2 Classify failures as build failure, timeout, no tests enabled, correctness failure, approximate recall threshold failure, or performance threshold failure.
- [ ] 7.3 Merge correctness report JSON/MD and datasource summary JSON into one final CLI summary.
- [ ] 7.4 Include profile artifact paths and pprof top summaries when profiling is enabled.

## 8. Presets And VSJoin Gate

- [x] 8.1 Add `--preset quick` for core unit tests plus a small correctness matrix.
- [x] 8.2 Add `--preset vsjoin` for VSJoin unit targets plus small VSJoin correctness and datasource sampling cases.
- [x] 8.3 Add `--preset datasource-smoke` for one generated case and one direct-load SIFT random sample case.
- [x] 8.4 Add `--preset perf-smoke` for bounded p sweep with fixed size/window and no full matrix.
- [x] 8.5 Ensure `full` is explicit and dry-run prints the expanded matrix before execution.

## 9. Integration Test Datasource Alignment

- [x] 9.1 Extend `IntegrationTestCase` with dataset sampling fields aligned with datasource-mode config.
- [x] 9.2 Reuse datasource sampling and split utilities in `join_baseline_integration_test.cpp` dataset mode.
- [x] 9.3 Reuse or mirror the shared similarity contract so integration expected/runtime alpha and mode cannot diverge.
- [x] 9.4 Add a small SIFT random-sampling integration case for bruteforce and VSJoin.

## 10. Verification

- [x] 10.1 Run datasource sampling unit tests.
- [x] 10.2 Run `test_join_datasource_modes` smoke for generated random and direct-load SIFT random sampling.
- [ ] 10.3 Run `scripts/run_integration_test.py --suite correctness --methods bruteforce ivf vsjoin --parallelism 1 2 --data-sizes 500`.
- [x] 10.4 Run `scripts/run_integration_test.py --suite datasource --methods bruteforce vsjoin --mode direct_load --data-source-file data/siftsmall/siftsmall_query.fvecs --sample-mode random --data-sizes 100 --parallelism 1`.
- [x] 10.5 Run `scripts/run_integration_test.py --preset vsjoin --dry-run` and one non-dry-run smoke.
- [x] 10.6 Confirm no checked-in config file is modified by runner execution.
- [x] 10.7 Confirm final `git status --short` contains only intended source/spec changes.
