"""Correctness-suite TOML filtering utilities."""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from sageflow_join_gate.methods import ALGORITHM_NAME_MAP

try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None


def load_toml_config(config_path: str) -> Dict[str, Any]:
    if tomllib is None:
        raise ImportError("需要 tomllib (Python 3.11+) 或 tomli 来解析 TOML 文件。")
    with open(config_path, "rb") as config_file:
        return tomllib.load(config_file)


def filter_test_cases(
    config: Dict[str, Any],
    methods: List[str],
    gtest_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    test_cases = config.get("test_case", [])
    if "all" in methods and not gtest_filter:
        return test_cases

    target_algorithms = set()
    if "all" not in methods:
        for method in methods:
            target_algorithms.update(ALGORITHM_NAME_MAP.get(method, []))

    filtered: List[Dict[str, Any]] = []
    for test_case in test_cases:
        if not test_case.get("enabled", True):
            continue
        if gtest_filter:
            if _matches_gtest_filter(test_case.get("name", ""), gtest_filter):
                filtered.append(test_case)
            continue
        if "all" in methods or test_case.get("algorithm", "") in target_algorithms:
            filtered.append(test_case)
    return filtered


def _matches_gtest_filter(test_name: str, gtest_filter: str) -> bool:
    for pattern in gtest_filter.split(":"):
        regex_pattern = pattern.replace(".", r"\.").replace("*", ".*").replace("?", ".")
        if re.match(f"^{regex_pattern}$", test_name):
            return True
    return False


def modify_test_cases(
    test_cases: List[Dict[str, Any]],
    parallelism: Optional[List[int]] = None,
    data_sizes: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    modified = []
    for test_case in test_cases:
        test_case_copy = dict(test_case)
        if parallelism is not None:
            test_case_copy["parallelism"] = list(parallelism)
        if data_sizes is not None:
            test_case_copy["data_sizes"] = list(data_sizes)
        modified.append(test_case_copy)
    return modified


def format_toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return "[" + ", ".join(format_toml_value(item) for item in value) + "]"
    return str(value)


def generate_temp_toml(
    config: Dict[str, Any],
    test_cases: List[Dict[str, Any]],
    output_path: Path,
    original_config_path: str,
    methods: List[str],
    gtest_filter: Optional[str],
    parallelism: Optional[List[int]],
    data_sizes: Optional[List[int]],
) -> Path:
    temp_path = output_path / "filtered_config.toml"
    with temp_path.open("w", encoding="utf-8") as config_file:
        config_file.write("# Auto-generated filtered configuration\n")
        config_file.write(f"# Generated at: {datetime.now().isoformat()}\n")
        config_file.write(f"# Original config: {original_config_path}\n")
        config_file.write(f"# Methods filter: {methods}\n")
        if gtest_filter:
            config_file.write(f"# GTest filter: {gtest_filter}\n")
        if parallelism:
            config_file.write(f"# Parallelism override: {parallelism}\n")
        if data_sizes:
            config_file.write(f"# Data sizes override: {data_sizes}\n")
        config_file.write(f"# Total test cases: {len(test_cases)}\n\n")

        if "common" in config:
            config_file.write("[common]\n")
            common = dict(config["common"])
            common["result_output_dir"] = str(output_path)
            for key, value in common.items():
                config_file.write(f"{key} = {format_toml_value(value)}\n")
            config_file.write("\n")

        for test_case in test_cases:
            config_file.write("\n[[test_case]]\n")
            for key, value in test_case.items():
                config_file.write(f"{key} = {format_toml_value(value)}\n")
    return temp_path
