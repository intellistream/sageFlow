"""Build helpers for selected Join gate suites."""

from __future__ import annotations

import multiprocessing
import subprocess
from pathlib import Path
from typing import List, Optional


def build_project(
    build_type: str = "Release",
    verbose: bool = False,
    targets: Optional[List[str]] = None,
) -> bool:
    print(f"\n{'=' * 60}")
    print(f"Building project ({build_type})...")
    print(f"{'=' * 60}")

    Path("build").mkdir(exist_ok=True)
    cmake_cmd = [
        "cmake",
        "-B",
        "build",
        f"-DCMAKE_BUILD_TYPE={build_type}",
        "-DBUILD_TESTING=ON",
        "-DSAGEFLOW_ENABLE_METRICS=ON",
    ]
    print(f"Running: {' '.join(cmake_cmd)}")
    configure = subprocess.run(cmake_cmd, capture_output=not verbose, text=True)
    if configure.returncode != 0:
        print("CMake configuration failed!")
        if not verbose and configure.stderr:
            print(configure.stderr)
        return False

    jobs = multiprocessing.cpu_count()
    build_cmd = ["cmake", "--build", "build"]
    if targets:
        build_cmd.extend(["--target", *targets])
    build_cmd.extend(["-j", str(jobs)])

    print(f"Running: {' '.join(build_cmd)}")
    build = subprocess.run(build_cmd, capture_output=not verbose, text=True)
    if build.returncode != 0:
        print("Build failed!")
        if not verbose and build.stderr:
            print(build.stderr[-2000:] if len(build.stderr) > 2000 else build.stderr)
        return False

    print("Build successful!")
    return True
