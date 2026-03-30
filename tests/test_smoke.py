"""Smoke tests for basic package availability."""

import sage_flow


def test_package_imports() -> None:
    """Package should import and expose version metadata."""
    assert isinstance(sage_flow.__version__, str)
    assert sage_flow.__version__


def test_public_symbols_exposed() -> None:
    """Core symbols from the native extension should be visible."""
    expected = {
        "StreamEnvironment",
        "Stream",
        "SimpleStreamSource",
        "VectorData",
        "VectorRecord",
        "DataType",
    }
    for name in expected:
        assert hasattr(sage_flow, name)
