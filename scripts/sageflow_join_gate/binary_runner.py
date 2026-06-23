"""Subprocess execution for gate test binaries."""

from __future__ import annotations

import os
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple


def run_test_binary(
    binary_path: str,
    gtest_filter: str,
    output_dir: str,
    config_path: str = "",
    timeout: int = 3600,
    verbose: bool = False,
    dry_run: bool = False,
    log_file: Optional[Path] = None,
) -> Tuple[bool, str, str]:
    if not Path(binary_path).exists():
        return False, "", f"Binary not found: {binary_path}"

    cmd = [binary_path]
    if gtest_filter:
        cmd.append(f"--gtest_filter={gtest_filter}")

    env = os.environ.copy()
    env["SAGEFLOW_TEST_OUTPUT_DIR"] = output_dir
    if config_path:
        env["SAGEFLOW_TEST_CONFIG_PATH"] = config_path
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    print(f"\n{'=' * 60}")
    print("Running tests...")
    print(f"{'=' * 60}")
    print(f"Binary: {binary_path}")
    if config_path:
        print(f"Config: {config_path}")
    if gtest_filter:
        print(f"Filter: {gtest_filter}")
    print(f"Output: {output_dir}")
    print(f"Command: {' '.join(cmd)}")

    if dry_run:
        print("\n[DRY RUN] Would execute the above command")
        return True, "", ""

    start_time = time.time()
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
        _write_binary_log(log_file, binary_path, config_path, gtest_filter, output_dir, cmd, result)
        elapsed = time.time() - start_time

        if verbose or result.returncode != 0:
            if result.stdout:
                print("\n--- STDOUT ---")
                print(result.stdout[-5000:] if len(result.stdout) > 5000 else result.stdout)
            if result.stderr:
                print("\n--- STDERR ---")
                print(result.stderr[-2000:] if len(result.stderr) > 2000 else result.stderr)

        print(f"\nTests completed in {elapsed:.1f}s")
        print("All tests PASSED!" if result.returncode == 0 else f"Some tests FAILED (return code: {result.returncode})")
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        print(f"\nTest execution timed out after {timeout}s!")
        return False, "", "Timeout"
    except OSError as exc:
        print(f"\nError running tests: {exc}")
        return False, "", str(exc)


def _write_binary_log(
    log_file: Optional[Path],
    binary_path: str,
    config_path: str,
    gtest_filter: str,
    output_dir: str,
    cmd: list[str],
    result: subprocess.CompletedProcess[str],
) -> None:
    if log_file is None:
        return
    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with log_file.open("w", encoding="utf-8") as output:
            output.write("# SageFlow Integration Test Binary Log\n")
            output.write(f"started_at={datetime.now().isoformat()}\n")
            output.write(f"binary={binary_path}\n")
            if config_path:
                output.write(f"config={config_path}\n")
            if gtest_filter:
                output.write(f"gtest_filter={gtest_filter}\n")
            output.write(f"output_dir={output_dir}\n")
            output.write(f"command={' '.join(cmd)}\n")
            output.write(f"returncode={result.returncode}\n")
            output.write("\n===== STDOUT =====\n")
            output.write(result.stdout or "")
            output.write("\n\n===== STDERR =====\n")
            output.write(result.stderr or "")
    except OSError as exc:
        print(f"Warning: failed to write binary log to {log_file}: {exc}")
