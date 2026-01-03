"""SageFlow - Vector-native stream processing engine for incremental semantic state snapshots."""

from ._version import __author__, __email__, __version__

try:
    from ._sage_flow import (
        DPWrapper,
        IndexConfig,
        PartitionType,
        QueryConfig,
        QueryResult,
        SageFlow,
        SemanticQueryEngine,
    )

    __all__ = [
        "__version__",
        "__author__",
        "__email__",
        "SageFlow",
        "SemanticQueryEngine",
        "QueryConfig",
        "IndexConfig",
        "QueryResult",
        "DPWrapper",
        "PartitionType",
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
