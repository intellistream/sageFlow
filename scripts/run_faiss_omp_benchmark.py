#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FAISS OMP Benchmark Script

Purpose:
    Run FAISS integration tests (OMP on/off) in a loop, collect performance data,
    and generate a statistical report.

Usage:
    python3 scripts/run_faiss_omp_benchmark.py --iterations 5 --test-filter "faiss"
"""

import subprocess
import json
import argparse
import sys
import os
import re
from pathlib import Path
from typing import List, Dict, Any
from collections import defaultdict
import statistics
import time

class BenchmarkRunner:
    def __init__(self, test_binary: str, iterations: int, test_filter: str):
        self.test_binary = test_binary
        self.iterations = iterations
        self.test_filter = test_filter
        self.results = defaultdict(list)
        
    def run_single_test(self, iteration: int) -> bool:
        cmd = [
            self.test_binary,
            f"--gtest_filter={self.test_filter}",
            "--gtest_color=yes"
        ]
        
        print(f"\n{'='*80}")
        print(f"Running iteration {iteration + 1}/{self.iterations}...")
        print(f"Command: {' '.join(cmd)}")
        print(f"{'='*80}\n")
        
        try:
            # Modified to stream output to stdout for monitoring
            result = subprocess.run(
                cmd,
                capture_output=False,
                text=True,
                timeout=600
            )
            
            if result.returncode == 0:
                print(f"✓ Iteration {iteration + 1} completed successfully")
                return True
            else:
                print(f"✗ Iteration {iteration + 1} failed (code: {result.returncode})")
                # stderr is already printed to console
                return False
                
        except subprocess.TimeoutExpired:
            print(f"✗ Iteration {iteration + 1} timed out")
            return False
        except Exception as e:
            print(f"✗ Iteration {iteration + 1} exception: {e}")
            return False
    
    def parse_csv_results(self, csv_dir: Path) -> Dict[str, List[Dict[str, Any]]]:
        results = defaultdict(list)
        
        # Iterate over all CSV files
        for csv_file in csv_dir.glob("*_results*.csv"):
            try:
                with open(csv_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    
                if len(lines) < 2:
                    continue
                    
                headers = lines[0].strip().split(',')
                
                # Parse data lines
                for line in lines[1:]:
                    parts = line.strip().split(',')
                    if len(parts) != len(headers):
                        continue
                        
                    row = {}
                    for i, h in enumerate(headers):
                        val = parts[i]
                        try:
                            if '.' in val:
                                row[h] = float(val)
                            else:
                                row[h] = int(val)
                        except ValueError:
                            row[h] = val
                    
                    # Use test name as key
                    test_name = row.get('test_name', 'unknown')
                    results[test_name].append(row)
                    
            except Exception as e:
                print(f"Error parsing {csv_file}: {e}")
                
        return results

    def run(self):
        print(f"Starting benchmark: {self.iterations} iterations")
        
        # Clean up old results
        result_dir = Path("test/result/integration")
        if result_dir.exists():
            for f in result_dir.glob("*_results*.csv"):
                f.unlink()
        else:
            result_dir.mkdir(parents=True, exist_ok=True)
            
        # Run loop
        success_count = 0
        for i in range(self.iterations):
            if self.run_single_test(i):
                success_count += 1
                # Rename CSVs to avoid overwriting
                for csv_file in result_dir.glob("*_results.csv"):
                    if "iter" not in csv_file.name:
                        new_name = csv_file.parent / f"{csv_file.stem}_iter{i}.csv"
                        csv_file.rename(new_name)
            
            # Small pause
            time.sleep(1)
            
        print(f"\nBenchmark completed. Success: {success_count}/{self.iterations}")
        
        # Parse all results
        self.results = self.parse_csv_results(result_dir)
        self.generate_report(result_dir)

    def generate_report(self, output_dir: Path):
        report_path = output_dir / "faiss_omp_benchmark_report.md"
        json_path = output_dir / "faiss_omp_benchmark_results.json"
        
        # Save raw data
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2)
            
        # Generate Markdown report
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("# FAISS OMP Benchmark Report\n\n")
            f.write(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Iterations: {self.iterations}\n\n")
            
            f.write("## Summary Statistics\n\n")
            f.write("| Test Name | Samples | Avg Time (ms) | Median Time (ms) | Stdev (ms) | Avg RPS |\n")
            f.write("|-----------|---------|---------------|------------------|------------|---------|\n")
            
            for test_name, runs in sorted(self.results.items()):
                times = [r.get('execution_time_ms', 0) for r in runs]
                rps_list = [r.get('throughput_rps', 0) for r in runs]
                
                if not times:
                    continue
                    
                avg_time = statistics.mean(times)
                median_time = statistics.median(times)
                stdev_time = statistics.stdev(times) if len(times) > 1 else 0
                avg_rps = statistics.mean(rps_list)
                
                f.write(f"| {test_name} | {len(times)} | {avg_time:.2f} | {median_time:.2f} | {stdev_time:.2f} | {avg_rps:.2f} |\n")
            
            self._write_comparison(f)
            
        print(f"\nReport generated: {report_path}")
        print(f"Raw data saved: {json_path}")

    def _write_comparison(self, f):
        # Identify pairs (omp_on vs omp_off)
        omp_on_tests = {}
        omp_off_tests = {}
        
        for name, runs in self.results.items():
            if "omp_on" in name:
                base_name = name.replace("_omp_on", "").replace("omp_on_", "")
                omp_on_tests[base_name] = runs
            elif "omp_off" in name:
                base_name = name.replace("_omp_off", "").replace("omp_off_", "")
                omp_off_tests[base_name] = runs
                
        common_tests = set(omp_on_tests.keys()) & set(omp_off_tests.keys())
        
        if not common_tests:
            f.write("\n\nNo paired OMP on/off tests detected.\n")
            return
        
        f.write("\n\n## OMP On vs Off Comparison\n\n")
        f.write("| Test | OMP State | Avg Time (ms) | Speedup |\n")
        f.write("|------|-----------|---------------|---------|\n")
        
        for base_name in sorted(common_tests):
            on_data = omp_on_tests[base_name]
            off_data = omp_off_tests[base_name]
            
            on_time = statistics.mean([r.get('execution_time_ms', 0) for r in on_data])
            off_time = statistics.mean([r.get('execution_time_ms', 0) for r in off_data])
            
            speedup = off_time / on_time if on_time > 0 else 0
            
            f.write(f"| {base_name} | ON | {on_time:.2f} | - |\n")
            f.write(f"| | OFF | {off_time:.2f} | {speedup:.2f}x |\n")

def main():
    parser = argparse.ArgumentParser(description="Run FAISS OMP Benchmark")
    parser.add_argument("--binary", default="build/bin/test_join_baseline_integration", help="Path to test binary")
    parser.add_argument("--iterations", type=int, default=5, help="Number of iterations")
    parser.add_argument("--test-filter", default="*faiss*", help="GTest filter")
    
    args = parser.parse_args()
    
    if not Path(args.binary).exists():
        print(f"Error: Binary not found at {args.binary}")
        sys.exit(1)
        
    runner = BenchmarkRunner(args.binary, args.iterations, args.test_filter)
    runner.run()

if __name__ == "__main__":
    main()
