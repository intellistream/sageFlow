#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SageFlow 测试报告聚合脚本

聚合多个 JSON 测试报告，生成汇总视图和趋势分析。

用法:
    python generate_test_summary.py <report_dir> [--output summary.json] [--format markdown|json|both]
    python generate_test_summary.py ./test_reports -o summary.json --format both

参数:
    report_dir          包含 JSON 报告的目录
    --output, -o        输出摘要文件路径（默认为 stdout）
    --format, -f        输出格式: json, markdown, both（默认 both）
    --quiet, -q         静默模式，不打印到控制台
    --ci                CI 模式，失败时返回非零退出码
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


def load_reports(report_dir: Path) -> list[dict[str, Any]]:
    """加载目录中的所有 JSON 报告"""
    reports = []
    for json_file in sorted(report_dir.glob("*.json")):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                data["_source_file"] = json_file.name
                reports.append(data)
        except json.JSONDecodeError as e:
            print(f"Warning: Failed to parse {json_file}: {e}", file=sys.stderr)
        except IOError as e:
            print(f"Warning: Failed to load {json_file}: {e}", file=sys.stderr)

    # 按生成时间排序
    reports.sort(key=lambda r: r.get("generated_at", ""))
    return reports


def generate_summary(reports: list[dict[str, Any]]) -> dict[str, Any]:
    """生成汇总统计"""
    summary = {
        "generated_at": datetime.now().isoformat(),
        "report_count": len(reports),
        "total_tests": 0,
        "total_passed": 0,
        "total_failed": 0,
        "total_skipped": 0,
        "algorithms": {},
        "reports": [],
    }

    for report in reports:
        report_summary = report.get("summary", {})
        summary["total_tests"] += report_summary.get("total_tests", 0)
        summary["total_passed"] += report_summary.get("passed", 0)
        summary["total_failed"] += report_summary.get("failed", 0)
        summary["total_skipped"] += report_summary.get("skipped", 0)

        # 记录每个报告的摘要
        summary["reports"].append({
            "source": report.get("_source_file", "unknown"),
            "name": report.get("report_name", "unknown"),
            "generated_at": report.get("generated_at", ""),
            "git_commit": report.get("git_commit", "unknown"),
            "total": report_summary.get("total_tests", 0),
            "passed": report_summary.get("passed", 0),
            "failed": report_summary.get("failed", 0),
        })

        # 聚合算法统计
        algo_results = report.get("algorithm_results", {})
        for algo, algo_data in algo_results.items():
            if algo not in summary["algorithms"]:
                summary["algorithms"][algo] = {
                    "test_count": 0,
                    "passed": 0,
                    "failed": 0,
                    "recalls": [],
                    "precisions": [],
                    "throughputs": [],
                }

            algo_summary = summary["algorithms"][algo]
            algo_summary["test_count"] += algo_data.get("test_count", 0)
            algo_summary["passed"] += algo_data.get("passed", 0)
            algo_summary["failed"] += algo_data.get("failed", 0)

            # 收集指标用于计算平均值
            if "avg_recall" in algo_data and algo_data["avg_recall"] > 0:
                algo_summary["recalls"].append(algo_data["avg_recall"])
            if "avg_precision" in algo_data and algo_data["avg_precision"] > 0:
                algo_summary["precisions"].append(algo_data["avg_precision"])
            if "avg_throughput" in algo_data and algo_data["avg_throughput"] > 0:
                algo_summary["throughputs"].append(algo_data["avg_throughput"])

    # 计算算法平均值
    for algo_data in summary["algorithms"].values():
        recalls = algo_data.pop("recalls")
        precisions = algo_data.pop("precisions")
        throughputs = algo_data.pop("throughputs")

        algo_data["avg_recall"] = sum(recalls) / len(recalls) if recalls else 0.0
        algo_data["avg_precision"] = sum(precisions) / len(precisions) if precisions else 0.0
        algo_data["avg_throughput"] = sum(throughputs) / len(throughputs) if throughputs else 0.0

        test_count = algo_data["test_count"]
        algo_data["pass_rate"] = algo_data["passed"] / test_count if test_count > 0 else 0.0

    # 计算整体通过率
    if summary["total_tests"] > 0:
        summary["overall_pass_rate"] = summary["total_passed"] / summary["total_tests"]
    else:
        summary["overall_pass_rate"] = 0.0

    return summary


def format_throughput(throughput: float) -> str:
    """格式化吞吐量为人类可读字符串"""
    if throughput >= 1_000_000:
        return f"{throughput / 1_000_000:.1f}M/s"
    if throughput >= 1_000:
        return f"{throughput / 1_000:.1f}K/s"
    return f"{throughput:.0f}/s"


def print_summary(summary: dict[str, Any]) -> None:
    """打印汇总到控制台"""
    print("\n" + "=" * 60)
    print("       Aggregated Test Report Summary")
    print("=" * 60 + "\n")

    print(f"Generated: {summary['generated_at']}")
    print(f"Reports analyzed: {summary['report_count']}")
    print(f"Total tests: {summary['total_tests']}")
    print(f"Total passed: {summary['total_passed']}")
    print(f"Total failed: {summary['total_failed']}")
    print(f"Total skipped: {summary['total_skipped']}")

    overall_rate = summary["overall_pass_rate"] * 100
    status = "✅" if overall_rate >= 100 else ("⚠️" if overall_rate >= 80 else "❌")
    print(f"Overall pass rate: {overall_rate:.1f}% {status}\n")

    print("Algorithm Performance Summary:")
    print("-" * 60)
    header = f"{'Algorithm':<15} {'Tests':<8} {'Pass%':<10} {'Recall':<10} {'Precision':<10} {'Throughput':<12}"
    print(header)
    print("-" * 60)

    for algo, data in sorted(summary["algorithms"].items()):
        recall = data.get("avg_recall", 0)
        precision = data.get("avg_precision", 0)
        throughput = data.get("avg_throughput", 0)
        pass_rate = data.get("pass_rate", 0) * 100
        tp_str = format_throughput(throughput)

        row = f"{algo:<15} {data['test_count']:<8} {pass_rate:<10.1f} {recall:<10.3f} {precision:<10.3f} {tp_str:<12}"
        print(row)

    print("\n" + "=" * 60 + "\n")

    # 打印各报告摘要
    if summary["reports"]:
        print("Individual Reports:")
        print("-" * 60)
        for report in summary["reports"]:
            status = "✅" if report["failed"] == 0 else "❌"
            print(f"  {status} {report['source']}: {report['passed']}/{report['total']} passed")
        print()


def generate_markdown(summary: dict[str, Any]) -> str:
    """生成 Markdown 格式的汇总报告"""
    lines = []
    lines.append("# SageFlow Aggregated Test Report\n")
    lines.append(f"**Generated**: {summary['generated_at']}  ")
    lines.append(f"**Reports Analyzed**: {summary['report_count']}\n")

    lines.append("## Overall Summary\n")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Total Tests | {summary['total_tests']} |")
    lines.append(f"| Passed | {summary['total_passed']} ✅ |")
    failed_marker = " ❌" if summary["total_failed"] > 0 else ""
    lines.append(f"| Failed | {summary['total_failed']}{failed_marker} |")
    lines.append(f"| Skipped | {summary['total_skipped']} |")
    lines.append(f"| Pass Rate | {summary['overall_pass_rate'] * 100:.1f}% |")
    lines.append("")

    lines.append("## Algorithm Performance\n")
    lines.append("| Algorithm | Tests | Pass Rate | Avg Recall | Avg Precision | Throughput |")
    lines.append("|-----------|-------|-----------|------------|---------------|------------|")

    for algo, data in sorted(summary["algorithms"].items()):
        pass_rate = data.get("pass_rate", 0) * 100
        status = "✅" if pass_rate >= 100 else ("⚠️" if pass_rate >= 80 else "❌")
        recall = data.get("avg_recall", 0)
        precision = data.get("avg_precision", 0)
        throughput = format_throughput(data.get("avg_throughput", 0))

        lines.append(
            f"| {algo} | {data['test_count']} | {pass_rate:.0f}%{status} | "
            f"{recall:.3f} | {precision:.3f} | {throughput} |"
        )

    lines.append("")

    if summary["reports"]:
        lines.append("## Individual Reports\n")
        lines.append("| Source | Name | Git Commit | Tests | Passed | Failed |")
        lines.append("|--------|------|------------|-------|--------|--------|")
        for report in summary["reports"]:
            status = "✅" if report["failed"] == 0 else "❌"
            lines.append(
                f"| {report['source']} | {report['name']} | {report['git_commit']} | "
                f"{report['total']} | {report['passed']} {status} | {report['failed']} |"
            )
        lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Aggregate SageFlow test reports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("report_dir", type=Path, help="Directory containing JSON reports")
    parser.add_argument("--output", "-o", type=Path, help="Output summary file path")
    parser.add_argument(
        "--format", "-f",
        choices=["json", "markdown", "both"],
        default="both",
        help="Output format (default: both)"
    )
    parser.add_argument("--quiet", "-q", action="store_true", help="Suppress console output")
    parser.add_argument(
        "--ci", action="store_true",
        help="CI mode: exit with non-zero code if any tests failed"
    )

    args = parser.parse_args()

    if not args.report_dir.exists():
        print(f"Error: Directory {args.report_dir} does not exist", file=sys.stderr)
        sys.exit(1)

    reports = load_reports(args.report_dir)

    if not reports:
        print(f"No valid JSON reports found in {args.report_dir}", file=sys.stderr)
        sys.exit(1)

    summary = generate_summary(reports)

    # 控制台输出
    if not args.quiet:
        print_summary(summary)

    # 文件输出
    if args.output:
        output_path = args.output

        if args.format in ("json", "both"):
            json_path = output_path.with_suffix(".json") if args.format == "both" else output_path
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            print(f"JSON summary written to {json_path}")

        if args.format in ("markdown", "both"):
            md_path = output_path.with_suffix(".md") if args.format == "both" else output_path
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(generate_markdown(summary))
            print(f"Markdown summary written to {md_path}")

    # CI 模式：失败时返回非零退出码
    if args.ci and summary["total_failed"] > 0:
        print(f"\nCI Check Failed: {summary['total_failed']} test(s) failed", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
