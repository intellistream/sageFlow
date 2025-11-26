#!/usr/bin/env python3
"""Compare dataset ground truth pairs with sink results.

Usage:
    python scripts/compare_ground_truth.py --dataset test/data/generated_test_data.json \
        --sink test/result/datasource_modes/perf_join_gen_save_load_random_ivf_eager_20_p1_w5.json \
        [--window-ms 5 --threshold 0.8 --records 20]
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

Pair = Tuple[int, int]


class VectorResolver:
    def __init__(self, vectors: List[List[float]], right_offset: Optional[int], modulo_base: Optional[int]):
        self.vectors = vectors
        self.count = len(vectors)
        self.right_offset = right_offset
        self.modulo_base = modulo_base

    def _resolve_index(self, uid: int) -> Optional[int]:
        if 1 <= uid <= self.count:
            return uid - 1
        if self.right_offset:
            candidate = uid - self.right_offset
            if 1 <= candidate <= self.count:
                return candidate - 1
            if self.modulo_base:
                candidate = uid + self.modulo_base - self.right_offset
                if 1 <= candidate <= self.count:
                    return candidate - 1
        return None

    def get_vector(self, uid: int) -> Optional[List[float]]:
        idx = self._resolve_index(uid)
        if idx is None:
            return None
        return self.vectors[idx]

    def similarity(self, uid_a: int, uid_b: int, alpha: float) -> Optional[float]:
        vec_a = self.get_vector(uid_a)
        vec_b = self.get_vector(uid_b)
        if vec_a is None or vec_b is None:
            return None
        if len(vec_a) != len(vec_b):
            return None
        dist_sq = 0.0
        for a, b in zip(vec_a, vec_b):
            diff = float(a) - float(b)
            dist_sq += diff * diff
        dist = math.sqrt(dist_sq)
        return math.exp(-alpha * dist)


def load_dataset_ground_truth(
    dataset_path: Path,
    window_ms: Optional[int],
    similarity_threshold: Optional[float],
    record_count: Optional[int],
) -> Tuple[Set[Pair], Dict, List[List[float]]]:
    with dataset_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    vectors = data.get("vectors")
    if not vectors:
        raise ValueError(f"Dataset {dataset_path} does not include 'vectors' needed for similarity computation")

    gt_sets = data.get("ground_truth_sets")
    if not gt_sets:
        raise ValueError(f"Dataset {dataset_path} does not contain ground_truth_sets")

    def matches(entry: Dict) -> bool:
        if window_ms is not None and entry.get("window_ms") != window_ms:
            return False
        if similarity_threshold is not None and abs(entry.get("similarity_threshold") - similarity_threshold) > 1e-9:
            return False
        if record_count is not None:
            entry_count = entry.get("record_count") or entry.get("pair_count")
            if entry_count not in (record_count, None):
                return False
        return True

    candidates = [entry for entry in gt_sets if matches(entry)]
    if not candidates:
        raise ValueError(
            "No ground truth entry matches the provided filters. "
            "Specify --window-ms/--threshold/--records to disambiguate."
        )
    if len(candidates) > 1:
        labels = ", ".join(entry.get("label", "<unnamed>") for entry in candidates)
        raise ValueError(
            "Multiple ground truth entries match the filters: " f"{labels}. Please narrow the criteria."
        )

    entry = candidates[0]
    return {(int(a), int(b)) for a, b in entry.get("pairs", [])}, entry, vectors


def load_sink_pairs(sink_path: Path) -> Set[Pair]:
    with sink_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    pairs = data.get("actual_pairs")
    if pairs is None:
        raise ValueError(f"Sink file {sink_path} missing 'actual_pairs' array")

    return {(int(a), int(b)) for a, b in pairs}


def compare_pairs(gt_pairs: Set[Pair], sink_pairs: Set[Pair]) -> Dict[str, Set[Pair]]:
    return {
        "missing": gt_pairs - sink_pairs,
        "unexpected": sink_pairs - gt_pairs,
        "matched": gt_pairs & sink_pairs,
    }


def format_pairs(
    pairs: Set[Pair],
    limit: int = 20,
    resolver: Optional[VectorResolver] = None,
    alpha: Optional[float] = None,
) -> str:
    if not pairs:
        return "<none>"
    sample = list(pairs)[:limit]
    lines: List[str] = []
    for a, b in sample:
        line = f"({a}, {b})"
        if resolver and alpha is not None:
            sim = resolver.similarity(a, b, alpha)
            if sim is not None:
                line += f" sim={sim:.6f}"
            else:
                line += " sim=?"
        lines.append(line)
    if len(pairs) > limit:
        lines.append(f"... (+{len(pairs) - limit} more)")
    return "\n  " + "\n  ".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare dataset ground truth with sink results")
    parser.add_argument("--dataset", required=True, type=Path, help="Path to dataset JSON (with ground_truth_sets)")
    parser.add_argument("--sink", required=True, type=Path, help="Path to sink result JSON")
    parser.add_argument("--window-ms", type=int, help="Window size (ms) to select ground truth entry")
    parser.add_argument("--threshold", type=float, help="Similarity threshold to select ground truth entry")
    parser.add_argument("--records", type=int, help="Record count to select ground truth entry")
    parser.add_argument("--show", type=int, default=20, help="Number of mismatched pairs to display")
    parser.add_argument("--alpha", type=float, help="Override similarity alpha (default: entry alpha or 0.1)")
    parser.add_argument("--right-offset", type=int, default=500000, help="UID offset used for right stream records")
    parser.add_argument("--modulo-base", type=int, help="Modulo base used when encoding right UID")
    args = parser.parse_args()

    if not args.dataset.exists():
        print(f"Dataset file not found: {args.dataset}", file=sys.stderr)
        return 1
    if not args.sink.exists():
        print(f"Sink file not found: {args.sink}", file=sys.stderr)
        return 1

    try:
        gt_pairs, gt_entry, vectors = load_dataset_ground_truth(
            args.dataset, args.window_ms, args.threshold, args.records
        )
        sink_pairs = load_sink_pairs(args.sink)
    except ValueError as err:
        print(f"Error: {err}", file=sys.stderr)
        return 1

    entry_alpha = gt_entry.get("alpha", 0.1)
    alpha = args.alpha if args.alpha is not None else entry_alpha
    modulo_base = args.modulo_base if args.modulo_base is not None else gt_entry.get("modulo_base")
    resolver = VectorResolver(vectors, args.right_offset, modulo_base)

    diff = compare_pairs(gt_pairs, sink_pairs)
    total_gt = len(gt_pairs)
    total_sink = len(sink_pairs)
    matched = len(diff["matched"])

    print("=== Ground Truth vs Sink Comparison ===")
    print(f"Dataset: {args.dataset}")
    print(f"Sink:    {args.sink}")
    print(f"GT pairs: {total_gt}")
    print(f"Sink pairs: {total_sink}")
    print(f"Matched pairs: {matched}")
    print(f"Missing pairs (in GT, not in sink): {len(diff['missing'])}")
    print(f"Unexpected pairs (in sink, not in GT): {len(diff['unexpected'])}")

    if diff["missing"]:
        print(f"\nMissing pair samples (limit {args.show}):")
        print(format_pairs(diff["missing"], args.show, resolver, alpha))
    if diff["unexpected"]:
        print(f"\nUnexpected pair samples (limit {args.show}):")
        print(format_pairs(diff["unexpected"], args.show, resolver, alpha))

    return 0 if matched == total_gt == total_sink else 2


if __name__ == "__main__":
    raise SystemExit(main())
