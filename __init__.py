"""Compatibility import surface for the repository root package."""

try:
	# Preferred modern layout used by this repository.
	from .sage_flow import *  # noqa: F401,F403
except ImportError:
	# Legacy layout kept for older downstream paths.
	from .python.sage_flow import *  # noqa: F401,F403  # type: ignore
