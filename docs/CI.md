# CI/CD with GitHub Actions

This repository includes a GitHub Actions workflow to build and test on every Pull Request and on pushes to main.

## Triggers
- pull_request targeting: main, develop, join_pre_experiment
- push to: main

## Platform & Configuration
- OS: ubuntu-latest
- Generator: Ninja
- Build type: Release
- Options:
  - BUILD_TESTING=ON
  - CANDY_ENABLE_METRICS=ON (collect metrics code in operators and tests)

## Pipeline Steps
1. Checkout
2. Setup Ninja and dependencies
3. Configure with CMake
4. Build with CMake (parallel)
5. Run tests using ctest
   - Unit tests: `-L UNIT`
   - Selected performance tests: regex `(test_join_perf_scaling|test_window_pipeline)` with label `PERF`

Artifacts are uploaded on failure: `build/Testing/**`, `build/*.log`, `build/**/compile_commands.json`.

## Adjusting Which Tests Run
- To run all tests (including Integration): change the ctest command in the workflow to remove label filters or add `-L INTEGRATION`.
- To select specific tests: edit the regex in the step "Run selected performance tests" or add more steps.

## Local Reproduction
To reproduce the CI locally (Ubuntu-like environment):

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON -DCANDY_ENABLE_METRICS=ON
cmake --build build --parallel
ctest --test-dir build --output-on-failure -L UNIT
ctest --test-dir build --output-on-failure -R "(test_join_perf_scaling|test_window_pipeline)" -L PERF
```

## Notes
- Performance tests can be long; we keep a small subset to balance signal and runtime. If they become flaky, consider gating them behind a separate workflow or nightly schedule.
- If your project adds more targets under `test/CMakeLists.txt`, they are automatically picked up by `ctest` via the `add_gtest` macro.
