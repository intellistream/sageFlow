"""Datasource-suite filtered TOML generation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Final


DEFAULT_DATASOURCE_CONFIG: Final[str] = "config/perf_join_datasource_modes.toml"
DEFAULT_DATASOURCE_BINARY: Final[str] = "build/bin/test_join_datasource_modes"


@dataclass(frozen=True)
class DatasourceConfigRequest:
    name: str
    mode: str
    methods: list[str]
    sizes: list[int]
    parallelism: list[int]
    window_time_ms: list[int]
    time_interval_ms: int
    similarity_threshold: float
    similarity_alpha: float
    similarity_mode: str
    split_mode: str
    data_source_type: str
    data_source_file: str
    expected_dim: int
    sample_mode: str
    sample_seed: int
    sample_offset: int
    sample_stride: int
    loop: bool
    vector_dim: int
    seed: int


def _format_value(value: str | int | float | bool | list[str] | list[int]) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    if isinstance(value, (int, float)):
        return str(value)
    return "[" + ", ".join(_format_value(item) for item in value) + "]"


def write_datasource_config(request: DatasourceConfigRequest, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    config_path = output_dir / "datasource_filtered_config.toml"
    with config_path.open("w", encoding="utf-8") as config_file:
        config_file.write("# Auto-generated datasource suite configuration\n")
        config_file.write("[log]\nlevel = \"info\"\n\n")
        config_file.write("[[performance_test]]\n")
        fields: list[tuple[str, str | int | float | bool | list[str] | list[int]]] = [
            ("name", request.name),
            ("mode", request.mode),
            ("methods", request.methods),
            ("sizes", request.sizes),
            ("records_count", request.sizes[0]),
            ("vector_dim", request.vector_dim),
            ("parallelism", request.parallelism),
            ("window_time_ms", request.window_time_ms),
            ("window_trigger_ms", 50),
            ("time_interval", request.time_interval_ms),
            ("similarity_threshold", request.similarity_threshold),
            ("similarity_alpha", request.similarity_alpha),
            ("similarity_mode", request.similarity_mode),
            ("split_mode", request.split_mode),
            ("seed", request.seed),
        ]
        for key, value in fields:
            config_file.write(f"{key} = {_format_value(value)}\n")

        config_file.write("\n[performance_test.data_source]\n")
        data_source_fields: list[tuple[str, str | int | bool]] = [
            ("type", request.data_source_type),
            ("file_path", request.data_source_file),
            ("expected_dim", request.expected_dim),
            ("loop", request.loop),
            ("sample_mode", request.sample_mode),
            ("sample_seed", request.sample_seed),
            ("sample_offset", request.sample_offset),
            ("sample_stride", request.sample_stride),
        ]
        for key, value in data_source_fields:
            config_file.write(f"{key} = {_format_value(value)}\n")
    return config_path
