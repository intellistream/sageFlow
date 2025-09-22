"""
SAGE Flow - High-performance vector stream processing engine

This module provides a Python interface to the candyFlow vector stream processing engine,
which supports real-time vector operations and semantic state snapshots.
"""

from typing import Any, Callable, Dict, List, Optional, Union

import numpy as np

try:
    from . import _sage_flow
except ImportError:
    # 尝试从本组件的本地构建目录加载扩展模块
    import importlib
    import sys
    from pathlib import Path

    here = Path(__file__).resolve().parent
    build_lib = here / "build" / "lib"
    if build_lib.exists():
        sys.path.insert(0, str(build_lib))
        try:
            _sage_flow = importlib.import_module("_sage_flow")
        except Exception as e:
            raise ImportError(
                "candyFlow C++ extension not available after adding build/lib to sys.path"
            ) from e
    else:
        raise ImportError(
            "candyFlow C++ extension not available. Please build the extension first."
        )

# Re-export C++ classes and enums (only those available in bindings)
DataType = _sage_flow.DataType
VectorData = _sage_flow.VectorData
VectorRecord = _sage_flow.VectorRecord
Stream = _sage_flow.Stream
StreamEnvironment = _sage_flow.StreamEnvironment
SimpleStreamSource = _sage_flow.SimpleStreamSource


class SageFlow:
    """
    High-performance vector stream processing engine.

    Supports real-time vector operations, windowed computations, and semantic state snapshots.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize a new SageFlow instance.

        Args:
            config: Configuration dictionary for the stream engine
        """
        self.env = StreamEnvironment()
        self.streams = []
        self.config = config or {}

    def create_stream(self, name):
        """
        Create a new data stream.

        Args:
            name: Name of the stream

        Returns:
            Stream object
        """
        return Stream(name)

    def create_simple_source(self, name):
        """
        Create a simple stream source for adding records programmatically.

        Args:
            name: Name of the source

        Returns:
            SimpleStreamSource object
        """
        return SimpleStreamSource(name)

    def add_vector_record(self, source, uid, timestamp, vector):
        """
        Add a vector record to a stream source.

        Args:
            source: The stream source to add to
            uid: Unique identifier for the record
            timestamp: Timestamp for the record
            vector: Vector data
        """
        if isinstance(vector, np.ndarray):
            vector = vector.tolist()
        source.addRecord(uid, timestamp, np.array(vector, dtype=np.float32))

    def process_stream(self, stream, operations):
        """
        Apply a series of operations to a stream.

        Args:
            stream: Input stream
            operations: List of operations to apply

        Returns:
            Processed stream
        """
        current_stream = stream

        for op in operations:
            op_type = op.get("type")
            if op_type == "filter":
                condition = op.get("condition")
                current_stream = current_stream.filter(condition)
            elif op_type == "map":
                transform = op.get("transform")
                current_stream = current_stream.map(transform)
            elif op_type == "window":
                window_size = op.get("window_size", 100)
                slide_size = op.get("slide_size", 50)
                current_stream = current_stream.window(window_size, slide_size)
            elif op_type == "itopk":
                k = op.get("k", 10)
                dim = op.get("dim", 128)
                current_stream = current_stream.itopk(k, dim)
            elif op_type == "topk":
                index_id = op.get("index_id", 0)
                k = op.get("k", 10)
                current_stream = current_stream.topk(index_id, k)
            elif op_type == "sink":
                sink_func = op.get("sink")
                current_stream = current_stream.writeSink(sink_func)

        return current_stream

    def add_stream(self, stream):
        """
        Add a stream to the environment.

        Args:
            stream: Stream to add
        """
        self.streams.append(stream)
        self.env.addStream(stream)

    def execute(self):
        """
        Execute all streams in the environment.
        """
        self.env.execute()

    def get_stream_snapshot(self) -> Dict[str, Any]:
        """
        Get current snapshot of stream processing state.

        Returns:
            Dictionary containing snapshot information
        """
        # candyFlow doesn't have a direct snapshot API, so we return basic info
        return {
            "streams_count": len(self.streams),
            "config": self.config,
            "status": "active",
        }


# Convenience functions
def create_stream_engine(config: Optional[Dict[str, Any]] = None) -> SageFlow:
    """Create a new SageFlow stream engine."""
    return SageFlow(config)


def create_vector_stream(name):
    """Create a new vector data stream."""
    return Stream(name)


def create_simple_data_source(name):
    """Create a simple data source for vector records."""
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

# Remove duplicate placeholder block below; keep the functional API above.
