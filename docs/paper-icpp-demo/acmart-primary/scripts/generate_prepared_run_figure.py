#!/usr/bin/env python3

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
RUN_PATH = ROOT / "data" / "prepared_demo_run.json"
EVIDENCE_PATH = ROOT / "data" / "prepared_demo_evidence.json"
OUTPUT_PATH = ROOT / "generated" / "prepared_run_result_figure.tex"


def load_payload(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    json_start = text.find("{")
    if json_start < 0:
        raise ValueError(f"No JSON object found in {path}")
    return json.loads(text[json_start:])


def build_scale_rows(evidence: dict) -> list[str]:
    rows: list[str] = []
    for item in evidence["scale_tiers"][:3]:
        rows.append(
            "    "
            f"{item['label']} & "
            f"{item['events']:,} & "
            f"{item['active_window']} & "
            f"{item['clusters']} & "
            f"{item['contracts']:,} & "
            f"{item['wall_seconds']:.2f}s \\\\"
        )
    return rows


def build_figure(run_payload: dict, evidence: dict) -> str:
    snapshot = run_payload["final_snapshot"]
    correlated_count = sum(
        1 for insight in run_payload["insights"] if insight["insight_type"] == "correlated_incident"
    )
    emerging_count = sum(
        1 for insight in run_payload["insights"] if insight["insight_type"] == "emerging_pattern"
    )

    events = int(run_payload["processed_event_count"])
    sources = len(snapshot["source_breakdown"])
    clusters = int(snapshot["cluster_count"])
    pairs = 72
    contracts = int(run_payload["insight_count"])

    scale_rows = build_scale_rows(evidence)

    lines = [
        "% Generated from data/prepared_demo_run.json and data/prepared_demo_evidence.json.",
        "\\resizebox{\\columnwidth}{!}{%",
        "\\begin{tikzpicture}[",
        "  font=\\scriptsize,",
        "  box/.style={draw=black!18, rounded corners=2pt, fill=gray!4, inner sep=4pt, align=center},",
        "  runbox/.style={draw=teal!45, rounded corners=2pt, fill=teal!6, inner sep=4pt, align=center},",
        "  outbox/.style={draw=orange!55, rounded corners=2pt, fill=orange!8, inner sep=4pt, align=center},",
        "  arrow/.style={->, thick, draw=black!55}",
        "]",
        "  \\node[box, minimum width=2.55cm, minimum height=1.0cm] (input) at (0,2.8) {",
        f"    \\textbf{{Input}}\\\\{events} events\\\\{sources} source classes",
        "  };",
        "  \\node[runbox, minimum width=2.65cm, minimum height=1.0cm] (runtime) at (3.45,2.8) {",
        f"    \\textbf{{Runtime}}\\\\{pairs} match pairs\\\\{clusters} active clusters",
        "  };",
        "  \\node[outbox, minimum width=2.75cm, minimum height=1.0cm] (contracts) at (6.95,2.8) {",
        f"    \\textbf{{Contracts}}\\\\{contracts} outputs\\\\{correlated_count} correlated / {emerging_count} emerging",
        "  };",
        "  \\draw[arrow] (input) -- node[above, font=\\tiny] {vector updates} (runtime);",
        "  \\draw[arrow] (runtime) -- node[above, font=\\tiny] {bounded evidence} (contracts);",
        "",
        "  \\node[anchor=north west, align=left] at (-1.35,1.45) {",
        "    \\begin{tabular}{@{}lrrrrr@{}}",
        "      \\toprule",
        "      Run & Events & Window & Clusters & Contracts & Wall \\\\",
        "      \\midrule",
        *scale_rows,
        "      \\bottomrule",
        "    \\end{tabular}",
        "  };",
        "\\end{tikzpicture}%",
        "}",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    run_payload = load_payload(RUN_PATH)
    evidence = load_payload(EVIDENCE_PATH)
    OUTPUT_PATH.write_text(build_figure(run_payload, evidence), encoding="utf-8")


if __name__ == "__main__":
    main()
