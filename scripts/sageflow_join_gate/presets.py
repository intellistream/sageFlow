"""Preset and suite-default handling for the Join gate CLI."""

from __future__ import annotations

import argparse

from sageflow_join_gate.datasource_config import (
    DEFAULT_DATASOURCE_BINARY,
    DEFAULT_DATASOURCE_CONFIG,
)
from sageflow_join_gate.suite_registry import (
    DEFAULT_CORRECTNESS_BINARY,
    DEFAULT_CORRECTNESS_CONFIG,
    DEFAULT_CORRECTNESS_OUTPUT_DIR,
    DEFAULT_DATASOURCE_OUTPUT_DIR,
)


def apply_preset(args: argparse.Namespace) -> None:
    if args.preset is None:
        return
    if args.preset == "quick":
        args.suite = "correctness"
        args.methods = ["bruteforce", "ivf", "vsjoin"]
        args.parallelism = args.parallelism or [1, 2]
        args.data_sizes = args.data_sizes or [500]
    elif args.preset == "vsjoin":
        args.suite = "vsjoin"
        args.methods = ["vsjoin"]
        args.parallelism = args.parallelism or [1, 2]
        args.data_sizes = args.data_sizes or [500]
    elif args.preset == "datasource-smoke":
        args.suite = "datasource"
        args.methods = ["bruteforce"]
        args.mode = "direct_load" if args.data_source_file else args.mode
        args.sample_mode = "random"
        args.parallelism = args.parallelism or [1]
        args.data_sizes = args.data_sizes or [100]
    elif args.preset == "perf-smoke":
        args.suite = "perf"
        args.methods = ["bruteforce", "ivf"]
        args.parallelism = args.parallelism or [1, 2, 4]
        args.data_sizes = args.data_sizes or [1000]
        args.window_time_ms = args.window_time_ms or [10000]
    elif args.preset == "full":
        args.suite = "all"


def configure_suite_defaults(args: argparse.Namespace) -> None:
    if args.suite in ("datasource", "perf"):
        if args.config == DEFAULT_CORRECTNESS_CONFIG:
            args.config = DEFAULT_DATASOURCE_CONFIG
        if args.binary_path == DEFAULT_CORRECTNESS_BINARY:
            args.binary_path = DEFAULT_DATASOURCE_BINARY
        if args.output_dir == DEFAULT_CORRECTNESS_OUTPUT_DIR:
            args.output_dir = DEFAULT_DATASOURCE_OUTPUT_DIR
    elif args.suite == "vsjoin" and args.methods == ["bruteforce"]:
        args.methods = ["vsjoin"]
