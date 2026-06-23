"""Join method filters used by the gate runner."""

from __future__ import annotations

from typing import Dict, List


METHOD_FILTER_MAP: Dict[str, str] = {
    "bruteforce": "*bruteforce*",
    "ivf": "*ivf*",
    "hnsw": "*hnsw*",
    "hdr_tree": "*hdr_tree*",
    "clustered_join": "*clustered*",
    "s3j": "*s3j*",
    "vsjoin": "*vsjoin*",
}

SUPPORTED_METHODS = list(METHOD_FILTER_MAP.keys()) + ["all"]

ALGORITHM_NAME_MAP: Dict[str, List[str]] = {
    "bruteforce": ["bruteforce"],
    "ivf": ["ivf"],
    "hnsw": ["hnsw"],
    "hdr_tree": ["hdr_tree"],
    "clustered_join": ["clustered_join"],
    "s3j": ["s3j"],
    "vsjoin": ["vsjoin"],
}


def build_gtest_filter(methods: List[str]) -> str:
    if "all" in methods:
        return ""
    filters = [METHOD_FILTER_MAP[method] for method in methods if method in METHOD_FILTER_MAP]
    return ":".join(filters)
