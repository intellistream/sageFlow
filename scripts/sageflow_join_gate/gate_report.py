"""Consolidated Join gate report writer."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class GateReport:
    suite: str
    command: list[str]
    binary: str
    effective_config: str
    output_dir: str
    return_code: int
    success: bool
    failure_classification: str
    artifacts: list[str]


def classify_failure(success: bool, stderr: str) -> str:
    if success:
        return "passed"
    if "Binary not found" in stderr:
        return "build failure"
    if "Timeout" in stderr:
        return "timeout"
    if "0 tests" in stderr:
        return "no tests enabled"
    if "Recall too low" in stderr or "recall" in stderr.lower():
        return "approximate recall threshold failure"
    return "correctness failure"


def write_gate_report(report: GateReport, run_dir: Path) -> Path:
    output_path = run_dir / "gate_report.json"
    with output_path.open("w", encoding="utf-8") as report_file:
        json.dump(asdict(report), report_file, indent=2, sort_keys=True)
        report_file.write("\n")
    return output_path
