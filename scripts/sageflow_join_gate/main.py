"""Main orchestration for the Join gate runner."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from sageflow_join_gate.binary_runner import run_test_binary
from sageflow_join_gate.build import build_project
from sageflow_join_gate.cli import parse_args
from sageflow_join_gate.config_filter import (
    filter_test_cases,
    generate_temp_toml,
    load_toml_config,
    modify_test_cases,
)
from sageflow_join_gate.datasource_config import (
    DatasourceConfigRequest,
    write_datasource_config,
)
from sageflow_join_gate.gate_report import GateReport, classify_failure, write_gate_report
from sageflow_join_gate.methods import build_gtest_filter
from sageflow_join_gate.presets import apply_preset, configure_suite_defaults
from sageflow_join_gate.results import (
    collect_datasource_summaries,
    collect_results,
    print_datasource_summary_paths,
    print_results_summary,
)
from sageflow_join_gate.suite_registry import build_targets_for_suite


def main() -> int:
    args = parse_args()
    apply_preset(args)
    configure_suite_defaults(args)
    _print_run_header(args)

    if args.build:
        build_targets = build_targets_for_suite(args.suite, args.full_build)
        if build_targets:
            print(f"Build targets: {build_targets}")
        if not build_project(args.build_type, args.verbose, build_targets):
            print("Build failed, exiting.")
            return 1

    run_dir = _create_run_dir(args.output_dir)
    gtest_filter = args.gtest_filter if args.gtest_filter else build_gtest_filter(args.methods)
    config_path = _effective_config_path(args, run_dir, gtest_filter)
    if config_path is None:
        return 1

    _write_runner_log(args, run_dir, config_path, gtest_filter)
    success, stdout, stderr = run_test_binary(
        binary_path=args.binary_path,
        gtest_filter=gtest_filter,
        output_dir=str(run_dir),
        config_path=config_path,
        timeout=args.timeout,
        verbose=args.verbose,
        dry_run=args.dry_run,
        log_file=run_dir / "logs" / "binary.log",
    )
    if args.dry_run:
        return 0

    artifacts = _print_and_collect_results(args.suite, run_dir, config_path)
    report_path = _write_gate_report(
        args=args,
        run_dir=run_dir,
        gtest_filter=gtest_filter,
        config_path=config_path,
        success=success,
        stdout=stdout,
        stderr=stderr,
        artifacts=artifacts,
    )
    print(f"Gate report: {report_path}")
    _run_visualization_if_requested(args, run_dir)
    return 0 if success else 1


def _print_run_header(args) -> None:
    print(f"\n{'=' * 60}")
    print("SageFlow Integration Test Runner")
    print(f"{'=' * 60}")
    print(f"Suite: {args.suite}")
    if args.preset:
        print(f"Preset: {args.preset}")
    print(f"Methods: {args.methods}")
    print(f"Config: {args.config}")
    if args.parallelism:
        print(f"Parallelism: {args.parallelism}")
    if args.data_sizes:
        print(f"Data sizes: {args.data_sizes}")
    print(f"Output directory: {args.output_dir} (will create per-run subfolder)")
    print(f"Binary path: {args.binary_path}")
    print(f"Visualize: {args.visualize}")


def _create_run_dir(output_dir: str) -> Path:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = Path(output_dir) / f"run_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _effective_config_path(args, run_dir: Path, gtest_filter: str) -> Optional[str]:
    if args.suite in ("datasource", "perf"):
        datasource_config = write_datasource_config(_datasource_request_from_args(args), run_dir)
        print(f"Generated datasource config: {datasource_config}")
        print("Datasource suite matrix:")
        print(f"  methods={args.methods}")
        print(f"  sizes={args.data_sizes or [1000]}")
        print(f"  parallelism={args.parallelism or [1]}")
        print(f"  window_time_ms={args.window_time_ms or [10000]}")
        print(f"  sample_mode={args.sample_mode} data_source={args.data_source_type}")
        return str(datasource_config)

    if not _needs_temp_config(args):
        return args.config
    try:
        print(f"\n{'=' * 60}")
        print("Generating filtered configuration...")
        print(f"{'=' * 60}")
        config = load_toml_config(args.config)
        print(f"Loaded config: {args.config}")
        print(f"Original test cases: {len(config.get('test_case', []))}")
        filtered_cases = filter_test_cases(config, args.methods, args.gtest_filter)
        print(f"After method/filter: {len(filtered_cases)} test cases")
        if not filtered_cases:
            print("Warning: No test cases match the specified filters!")
        modified_cases = modify_test_cases(filtered_cases, args.parallelism, args.data_sizes)
        temp_config = generate_temp_toml(
            config=config,
            test_cases=modified_cases,
            output_path=run_dir,
            original_config_path=args.config,
            methods=args.methods,
            gtest_filter=gtest_filter,
            parallelism=args.parallelism,
            data_sizes=args.data_sizes,
        )
        print(f"Generated temp config: {temp_config}")
        return str(temp_config)
    except ImportError as exc:
        print(f"Warning: Cannot generate temp config - {exc}")
        print("Falling back to original config with gtest_filter only.")
        return args.config
    except FileNotFoundError as exc:
        print(f"Error: Config file not found - {exc}")
        return None
    except OSError as exc:
        print(f"Warning: Failed to generate temp config - {exc}")
        print("Falling back to original config with gtest_filter only.")
        return args.config


def _needs_temp_config(args) -> bool:
    return (
        args.parallelism is not None
        or args.data_sizes is not None
        or "all" not in args.methods
        or args.gtest_filter is not None
    )


def _datasource_request_from_args(args) -> DatasourceConfigRequest:
    sizes = args.data_sizes or [1000]
    methods = [method for method in args.methods if method != "all"] or ["bruteforce"]
    return DatasourceConfigRequest(
        name="gate_datasource",
        mode=args.mode,
        methods=methods,
        sizes=sizes,
        parallelism=args.parallelism or [1],
        window_time_ms=args.window_time_ms or [10000],
        time_interval_ms=args.time_interval_ms,
        similarity_threshold=args.similarity_threshold,
        similarity_alpha=args.similarity_alpha,
        similarity_mode=args.similarity_mode,
        split_mode=args.split_mode,
        data_source_type=args.data_source_type,
        data_source_file=args.data_source_file,
        expected_dim=args.expected_dim,
        sample_mode=args.sample_mode,
        sample_seed=args.sample_seed,
        sample_offset=args.sample_offset,
        sample_stride=args.sample_stride,
        loop=True,
        vector_dim=args.expected_dim,
        seed=args.sample_seed,
    )


def _write_runner_log(args, run_dir: Path, config_path: str, gtest_filter: str) -> None:
    runner_log = run_dir / "logs" / "runner.log"
    runner_log.parent.mkdir(parents=True, exist_ok=True)
    with runner_log.open("w", encoding="utf-8") as log_file:
        log_file.write("# SageFlow Integration Test Runner Log\n")
        log_file.write(f"started_at={datetime.now().isoformat()}\n")
        log_file.write(f"methods={args.methods}\n")
        log_file.write(f"original_config={args.config}\n")
        log_file.write(f"effective_config={config_path}\n")
        log_file.write(f"output_dir={str(run_dir)}\n")
        log_file.write(f"binary_path={args.binary_path}\n")
        log_file.write(f"visualize={args.visualize}\n")
        log_file.write(f"gtest_filter={gtest_filter}\n")


def _print_and_collect_results(suite: str, run_dir: Path, config_path: str) -> list[str]:
    artifacts = [config_path]
    results = collect_results(str(run_dir))
    if results:
        print_results_summary(results)
    elif suite in ("datasource", "perf"):
        print_datasource_summary_paths(collect_datasource_summaries())
    else:
        print("\nNo result files found.")

    artifacts.extend(str(path) for path in run_dir.glob("*report*"))
    if suite in ("datasource", "perf"):
        artifacts.extend(collect_datasource_summaries())
    return artifacts


def _write_gate_report(
    args,
    run_dir: Path,
    gtest_filter: str,
    config_path: str,
    success: bool,
    stdout: str,
    stderr: str,
    artifacts: list[str],
) -> Path:
    gate_report = GateReport(
        suite=args.suite,
        command=[args.binary_path] + ([f"--gtest_filter={gtest_filter}"] if gtest_filter else []),
        binary=args.binary_path,
        effective_config=config_path,
        output_dir=str(run_dir),
        return_code=0 if success else 1,
        success=success,
        failure_classification=classify_failure(success, stderr + "\n" + stdout),
        artifacts=artifacts,
    )
    return write_gate_report(gate_report, run_dir)


def _run_visualization_if_requested(args, run_dir: Path) -> None:
    if not args.visualize:
        return
    try:
        from visualize_results import generate_charts

        generate_charts(str(run_dir), str(run_dir), args.chart_format)
    except ImportError as exc:
        print(f"Warning: Could not import visualization module: {exc}")
    except OSError as exc:
        print(f"Warning: Visualization failed: {exc}")


if __name__ == "__main__":
    sys.exit(main())
