#!/usr/bin/env python3
"""Backward-compatible entrypoint for the SageFlow Join gate runner."""

from __future__ import annotations

import sys

from sageflow_join_gate.main import main


if __name__ == "__main__":
    sys.exit(main())
