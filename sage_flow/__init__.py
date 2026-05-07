"""SageFlow - Vector-native stream processing engine for incremental semantic state snapshots."""

from ._version import __author__, __email__, __version__

try:
    from ._sage_flow import (
        # Data Types
        DataType,
        VectorData,
        VectorRecord,
        PersistentJoinPair,
        # Enums
        FunctionType,
        WindowType,
        AggregateType,
        # Function Classes
        Function,
        FilterFunction,
        MapFunction,
        JoinFunction,
        WindowFunction,
        AggregateFunction,
        TopkFunction,
        ITopkFunction,
        SinkFunction,
        # Stream Classes
        Stream,
        SimpleStreamSource,
        StreamingSource,  # 新增：支持动态流式输入
        StreamEnvironment,
        PersistentVectorJoinRuntime,
        # Convenience Functions
        create_source,
        create_streaming_source,  # 新增：创建 StreamingSource
        create_environment,
    )

    __all__ = [
        "__version__",
        "__author__",
        "__email__",
        # Data Types
        "DataType",
        "VectorData",
        "VectorRecord",
        "PersistentJoinPair",
        # Enums
        "FunctionType",
        "WindowType",
        "AggregateType",
        # Function Classes
        "Function",
        "FilterFunction",
        "MapFunction",
        "JoinFunction",
        "WindowFunction",
        "AggregateFunction",
        "TopkFunction",
        "ITopkFunction",
        "SinkFunction",
        # Stream Classes
        "Stream",
        "SimpleStreamSource",
        "StreamingSource",  # 新增
        "StreamEnvironment",
        "PersistentVectorJoinRuntime",
        # Convenience Functions
        "create_source",
        "create_streaming_source",  # 新增
        "create_environment",
    ]
except ImportError as e:
    import warnings

    warnings.warn(
        f"Failed to import C++ extension module: {e}\n"
        "SageFlow requires compilation. Install from source or use pre-built wheels.",
        ImportWarning,
        stacklevel=2,
    )
    __all__ = ["__version__", "__author__", "__email__"]
