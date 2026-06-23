"""Result collection and CLI summary rendering."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List


def collect_datasource_summaries() -> List[str]:
    result_dir = Path("test/result/datasource_modes")
    return [str(path) for path in sorted(result_dir.glob("*_summary.json"))]


def collect_results(output_dir: str) -> Dict:
    results: Dict = {}
    output_path = Path(output_dir)
    for json_file in output_path.glob("*report*.json"):
        _collect_json_report(json_file, results)

    if not results:
        for json_file in Path("test/result/integration").glob("*report*.json"):
            _collect_json_report(json_file, results)
    return results


def _collect_json_report(json_file: Path, results: Dict) -> None:
    try:
        with json_file.open("r", encoding="utf-8") as report_file:
            data = json.load(report_file)
            key = json_file.stem.replace("_report", "").replace("report", "main")
            results[key] = data
    except json.JSONDecodeError as exc:
        print(f"Warning: Failed to parse {json_file}: {exc}")
    except OSError as exc:
        print(f"Warning: Error reading {json_file}: {exc}")


def print_results_summary(results: Dict) -> None:
    print(f"\n{'=' * 60}")
    print("Results Summary")
    print(f"{'=' * 60}")
    for key, data in results.items():
        if "summary" in data:
            summary = data["summary"]
            print(f"\n{key}:")
            print(f"  Total:  {summary.get('total_tests', 0)}")
            print(f"  Passed: {summary.get('passed', 0)}")
            print(f"  Failed: {summary.get('failed', 0)}")
            print(f"  Skipped: {summary.get('skipped', 0)}")
        if "algorithm_results" in data:
            print("\n  Algorithm Performance:")
            for algorithm, stats in data["algorithm_results"].items():
                avg_recall = stats.get("avg_recall", 0)
                avg_throughput = stats.get("avg_throughput", 0)
                print(f"    {algorithm}: recall={avg_recall:.4f}, throughput={avg_throughput:.1f} rec/s")


def print_datasource_summary_paths(paths: List[str]) -> None:
    print(f"\nDatasource summaries: {len(paths)}")
    for summary_path in paths[-5:]:
        print(f"  {summary_path}")
