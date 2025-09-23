"""
SAGE Flow - High-performance vector stream processing engine (Python side)

All Python-facing APIs for SAGE-Flow live under this module.
"""

from typing import Any, Callable, Dict, Optional

import numpy as np

try:
    from . import _sage_flow
except ImportError:
    import importlib
    import sys
    from pathlib import Path

    here = Path(__file__).resolve().parent
    # In repo build, lib is located at ../build/lib relative to project root; adjust upwards
    candidate_paths = [
        here / "build" / "lib",  # standard local build
        here.parent / "build" / "lib",  # component-level build
    ]
    for p in candidate_paths:
        if p.exists():
            sys.path.insert(0, str(p))
    _sage_flow = importlib.import_module("_sage_flow")

DataType = _sage_flow.DataType
VectorData = _sage_flow.VectorData
VectorRecord = _sage_flow.VectorRecord
Stream = _sage_flow.Stream
StreamEnvironment = _sage_flow.StreamEnvironment
SimpleStreamSource = _sage_flow.SimpleStreamSource


class SageFlow:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.env = StreamEnvironment()
        self.streams = []
        self.config = config or {}

    def create_stream(self, name: str):
        return Stream(name)

    def create_simple_source(self, name: str):
        return SimpleStreamSource(name)

    def add_vector_record(self, source, uid: int, timestamp: int, vector):
        if isinstance(vector, np.ndarray):
            vector = vector.astype(np.float32, copy=False)
        else:
            vector = np.asarray(vector, dtype=np.float32)
        source.addRecord(uid, timestamp, vector)

    def add_stream(self, stream):
        self.streams.append(stream)
        self.env.addStream(stream)

    def execute(self):
        self.env.execute()

    def get_stream_snapshot(self) -> Dict[str, Any]:
        return {
            "streams_count": len(self.streams),
            "config": self.config,
            "status": "active",
        }


def create_stream_engine(config: Optional[Dict[str, Any]] = None) -> SageFlow:
    return SageFlow(config)


def create_vector_stream(name: str):
    return Stream(name)


def create_simple_data_source(name: str):
    return SimpleStreamSource(name)


__all__ = [
    "SageFlow",
    "create_stream_engine",
    "create_vector_stream",
    "create_simple_data_source",
    "DataType",
    "VectorData",
    "VectorRecord",
    "Stream",
    "StreamEnvironment",
    "SimpleStreamSource",
]
