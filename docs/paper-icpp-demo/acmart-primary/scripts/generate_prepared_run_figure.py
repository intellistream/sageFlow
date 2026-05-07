#!/usr/bin/env python3

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = ROOT / "data" / "prepared_demo_run.json"
OUTPUT_PATH = ROOT / "generated" / "prepared_run_result_figure.tex"


def load_payload(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    json_start = text.find("{")
    if json_start < 0:
        raise ValueError(f"No JSON object found in {path}")
    return json.loads(text[json_start:])


def y_top(base: float, value: int, scale: float = 0.45) -> float:
    return base + (value * scale)


def build_figure(payload: dict) -> str:
    snapshot = payload["final_snapshot"]
    clusters = sorted(snapshot["hot_clusters"], key=lambda item: item["cluster_id"])
    cluster_sizes = [int(cluster["size"]) for cluster in clusters]
    cluster_labels = [f"C{index}" for index, _ in enumerate(clusters, start=1)]

    correlated_count = sum(
        1
        for insight in payload["insights"]
        if insight["insight_type"] == "correlated_incident"
    )
    emerging_count = sum(
        1
        for insight in payload["insights"]
        if insight["insight_type"] == "emerging_pattern"
    )

    input_events = int(payload["processed_event_count"])
    distinct_sources = len(snapshot["source_breakdown"])
    latest_anomaly = snapshot["latest_event_id"]

    cluster_bar_lines: list[str] = []
    cluster_label_lines: list[str] = []
    cluster_value_lines: list[str] = []
    for index, (label, size) in enumerate(zip(cluster_labels, cluster_sizes), start=0):
        center_x = 0.6 + (index * 0.75)
        left_x = center_x - 0.24
        right_x = center_x + 0.24
        cluster_label_lines.append(
            f"    \\node[font=\\scriptsize] at ({center_x:.1f},4.8) {{{label}}};"
        )
        cluster_bar_lines.append(
            f"    \\draw[bar] ({left_x:.1f},5.2) rectangle ({right_x:.1f},{y_top(5.2, size):.1f});"
        )
        cluster_value_lines.append(
            f"    \\node[font=\\scriptsize] at ({center_x:.1f},{y_top(5.45, size):.1f}) {{{size}}};"
        )

    max_insight_count = max(correlated_count, emerging_count, 1)
    insight_scale = min(0.45, 2.5 / max_insight_count)

    lines = [
        "% Generated from data/prepared_demo_run.json by scripts/generate_prepared_run_figure.py.",
        "\\begin{tikzpicture}[",
        "  font=\\footnotesize,",
        "  x=0.9cm,",
        "  y=0.5cm,",
        "  bar/.style={draw, fill=blue!35},",
        "  ibar/.style={draw, fill=red!30}",
        "]",
        "  \\node[font=\\scriptsize] at (1.5,8.4) {Cluster size};",
        "  \\draw[->] (0,5.2) -- (5.4,5.2);",
        "  \\draw[->] (0,5.2) -- (0,8.0);",
        *cluster_label_lines,
        *cluster_bar_lines,
        *cluster_value_lines,
        "",
        "  \\node[font=\\scriptsize] at (7.0,8.4) {Insight count};",
        "  \\draw[->] (5.0,5.2) -- (8.8,5.2);",
        "  \\draw[->] (5.0,5.2) -- (5.0,8.0);",
        "  \\node[font=\\scriptsize, align=center] at (6.0,4.8) {Corr.};",
        "  \\node[font=\\scriptsize, align=center] at (7.4,4.8) {Emerg.};",
        f"  \\draw[ibar] (5.6,5.2) rectangle (6.4,{y_top(5.2, correlated_count, insight_scale):.1f});",
        f"  \\draw[ibar] (7.0,5.2) rectangle (7.8,{y_top(5.2, emerging_count, insight_scale):.1f});",
        f"  \\node[font=\\scriptsize] at (6.0,{y_top(5.5, correlated_count, insight_scale):.1f}) {{{correlated_count}}};",
        f"  \\node[font=\\scriptsize] at (7.4,{y_top(5.5, emerging_count, insight_scale):.1f}) {{{emerging_count}}};",
        "",
        "  \\node[draw, rounded corners, align=left, font=\\scriptsize, fill=gray!8] at (4.4,1.8) {",
        f"    Input events: {input_events}\\",
        f"    Distinct sources: {distinct_sources}\\",
        f"    Latest anomaly: {latest_anomaly}",
        "  };",
        "\\end{tikzpicture}",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    payload = load_payload(INPUT_PATH)
    OUTPUT_PATH.write_text(build_figure(payload), encoding="utf-8")


if __name__ == "__main__":
    main()
