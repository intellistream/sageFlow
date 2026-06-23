"""Suite metadata for the Join gate runner."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from sageflow_join_gate.datasource_config import (
    DEFAULT_DATASOURCE_BINARY,
    DEFAULT_DATASOURCE_CONFIG,
)


DEFAULT_CORRECTNESS_BINARY = "build/bin/test_join_baseline_integration"
DEFAULT_CORRECTNESS_CONFIG = "config/integration_test_cases.toml"
DEFAULT_CORRECTNESS_OUTPUT_DIR = "test/result/integration"
DEFAULT_DATASOURCE_OUTPUT_DIR = "test/result/datasource_modes_gate"
SUITE_CHOICES = ["correctness", "datasource", "vsjoin", "perf", "all"]


@dataclass(frozen=True)
class SuiteDefinition:
    name: str
    binary_path: str
    default_config: str
    output_dir: str
    build_targets: List[str]


SUITE_REGISTRY: Dict[str, SuiteDefinition] = {
    "correctness": SuiteDefinition(
        name="correctness",
        binary_path=DEFAULT_CORRECTNESS_BINARY,
        default_config=DEFAULT_CORRECTNESS_CONFIG,
        output_dir=DEFAULT_CORRECTNESS_OUTPUT_DIR,
        build_targets=["test_join_baseline_integration"],
    ),
    "datasource": SuiteDefinition(
        name="datasource",
        binary_path=DEFAULT_DATASOURCE_BINARY,
        default_config=DEFAULT_DATASOURCE_CONFIG,
        output_dir=DEFAULT_DATASOURCE_OUTPUT_DIR,
        build_targets=["test_join_datasource_modes"],
    ),
    "perf": SuiteDefinition(
        name="perf",
        binary_path=DEFAULT_DATASOURCE_BINARY,
        default_config=DEFAULT_DATASOURCE_CONFIG,
        output_dir=DEFAULT_DATASOURCE_OUTPUT_DIR,
        build_targets=["test_join_datasource_modes"],
    ),
    "vsjoin": SuiteDefinition(
        name="vsjoin",
        binary_path=DEFAULT_CORRECTNESS_BINARY,
        default_config=DEFAULT_CORRECTNESS_CONFIG,
        output_dir=DEFAULT_CORRECTNESS_OUTPUT_DIR,
        build_targets=[
            "test_vsjoin_factory",
            "test_vsjoin_method",
            "test_vsjoin_operator_path",
            "test_vsjoin_routing",
            "test_vsjoin_rebuild",
            "test_vsjoin_load_balancing",
            "test_partition_assignment",
            "test_load_monitor",
            "test_join_baseline_integration",
        ],
    ),
}


def suite_definition(name: str) -> Optional[SuiteDefinition]:
    return SUITE_REGISTRY.get(name)


def build_targets_for_suite(name: str, full_build: bool) -> Optional[List[str]]:
    if full_build:
        return None
    definition = suite_definition(name)
    if definition is None:
        return None
    return list(definition.build_targets)
