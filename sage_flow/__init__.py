"""SageFlow - Vector-native stream processing engine for incremental semantic state snapshots."""

from ._version import __author__, __email__, __version__

try:
    from ._sage_flow import (
        DataType,
        SimpleStreamSource,
        Stream,
        StreamEnvironment,
        VectorData,
        VectorRecord,
    )

    __all__ = [
        "__version__",
        "__author__",
        "__email__",
        "StreamEnvironment",
        "Stream",
        "SimpleStreamSource",
        "VectorData",
        "VectorRecord",
        "DataType",
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
