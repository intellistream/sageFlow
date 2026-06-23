"""Argument parsing for the Join gate runner."""

from __future__ import annotations

import argparse

from sageflow_join_gate.methods import SUPPORTED_METHODS
from sageflow_join_gate.suite_registry import (
    DEFAULT_CORRECTNESS_BINARY,
    DEFAULT_CORRECTNESS_CONFIG,
    DEFAULT_CORRECTNESS_OUTPUT_DIR,
    SUITE_CHOICES,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run SageFlow integration tests with specific Join methods",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --methods bruteforce ivf
  %(prog)s --methods all --parallelism 1 2 4 8 --visualize
  %(prog)s --methods hdr_tree --data-sizes 1000 2000 --output-dir results/
        """,
    )
    _add_selection_args(parser)
    _add_output_args(parser)
    _add_build_args(parser)
    _add_runtime_args(parser)
    _add_datasource_args(parser)
    return parser.parse_args()


def _add_selection_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--suite", choices=SUITE_CHOICES, default="correctness")
    parser.add_argument(
        "--preset",
        choices=["quick", "vsjoin", "datasource-smoke", "perf-smoke", "full"],
        default=None,
    )
    parser.add_argument(
        "--methods",
        "-m",
        nargs="+",
        choices=SUPPORTED_METHODS,
        default=["bruteforce"],
    )
    parser.add_argument("--gtest-filter", type=str, default=None)
    parser.add_argument("--parallelism", "-p", nargs="+", type=int, default=None)
    parser.add_argument("--data-sizes", "-d", nargs="+", type=int, default=None)


def _add_output_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output-dir", "-o", type=str, default=DEFAULT_CORRECTNESS_OUTPUT_DIR)
    parser.add_argument("--config", "-c", type=str, default=DEFAULT_CORRECTNESS_CONFIG)
    parser.add_argument("--binary-path", "-b", type=str, default=DEFAULT_CORRECTNESS_BINARY)
    parser.add_argument("--visualize", "-v", action="store_true")
    parser.add_argument("--chart-format", type=str, default="png", choices=["png", "svg", "pdf"])


def _add_build_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--full-build", action="store_true")
    parser.add_argument(
        "--build-type",
        type=str,
        default="Release",
        choices=["Debug", "Release", "RelWithDebInfo"],
    )


def _add_runtime_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--timeout", type=int, default=3600)


def _add_datasource_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--mode",
        default="generate_direct_use",
        choices=["generate_save_load", "direct_load", "generate_direct_use"],
    )
    parser.add_argument("--data-source-type", default="random", choices=["random", "dataset", "json"])
    parser.add_argument("--data-source-file", default="")
    parser.add_argument("--expected-dim", type=int, default=128)
    parser.add_argument("--split-mode", default="duplicate", choices=["duplicate", "half_split", "interleaved"])
    parser.add_argument("--sample-mode", default="sequential", choices=["sequential", "random", "stride"])
    parser.add_argument("--sample-seed", type=int, default=42)
    parser.add_argument("--sample-offset", type=int, default=0)
    parser.add_argument("--sample-stride", type=int, default=1)
    parser.add_argument("--similarity-alpha", type=float, default=0.1)
    parser.add_argument("--similarity-threshold", type=float, default=0.8)
    parser.add_argument(
        "--similarity-mode",
        default="fixed_alpha",
        choices=["fixed_alpha", "adaptive_alpha", "normalized"],
    )
    parser.add_argument("--window-time-ms", nargs="+", type=int, default=None)
    parser.add_argument("--time-interval-ms", type=int, default=10)
