#!/usr/bin/env python3
"""
Join算子测试运行器
根据test-instruction.md的要求运行完整的测试套件
"""

import subprocess
import sys
import os
import json
import time
from pathlib import Path
import argparse

class JoinTestRunner:
    def __init__(self, build_dir="build", config_dir="config"):
        self.build_dir = Path(build_dir)
        self.config_dir = Path(config_dir) 
        self.results = {}
        
    def run_unit_tests(self):
        """运行单元测试 - 高优先级：Join方法正确性测试"""
        print("=== Running Join Method Correctness Tests (High Priority) ===")
        
        unit_tests = [
            "test_join_bruteforce",
            "test_join_ivf", 
            "test_join_baseline"
        ]
        
        for test in unit_tests:
            print(f"\n--- Running {test} ---")
            try:
                result = subprocess.run([
                    str(self.build_dir / "test" / test)
                ], capture_output=True, text=True, timeout=300)
                
                self.results[test] = {
                    "returncode": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                    "passed": result.returncode == 0
                }
                
                if result.returncode == 0:
                    print(f"✓ {test} PASSED")
                else:
                    print(f"✗ {test} FAILED")
                    print(f"Error: {result.stderr}")
                    
            except subprocess.TimeoutExpired:
                print(f"✗ {test} TIMEOUT")
                self.results[test] = {"passed": False, "error": "timeout"}
            except FileNotFoundError:
                print(f"✗ {test} NOT FOUND (may need to build)")
                self.results[test] = {"passed": False, "error": "not_found"}
                
    def run_performance_tests(self):
        """运行性能测试 - 高优先级：Join算子性能测试"""
        print("\n=== Running Join Performance Tests (High Priority) ===")
        
        print("--- Running Scaling Performance Test ---")
        try:
            result = subprocess.run([
                str(self.build_dir / "test" / "test_join_perf_scaling")
            ], capture_output=True, text=True, timeout=600)
            
            self.results["performance_scaling"] = {
                "returncode": result.returncode,
                "stdout": result.stdout, 
                "stderr": result.stderr,
                "passed": result.returncode == 0
            }
            
            if result.returncode == 0:
                print("✓ Performance scaling tests PASSED")
            else:
                print("✗ Performance scaling tests FAILED")
                print(f"Error: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            print("✗ Performance tests TIMEOUT")
            self.results["performance_scaling"] = {"passed": False, "error": "timeout"}
        except FileNotFoundError:
            print("✗ Performance test executable NOT FOUND")
            self.results["performance_scaling"] = {"passed": False, "error": "not_found"}
            
    def run_integration_tests(self):
        """运行集成测试 - 高优先级：多线程Pipeline流程测试"""
        print("\n=== Running Multi-thread Pipeline Tests (High Priority) ===")
        
        print("--- Running Pipeline Integration Test ---")
        try:
            result = subprocess.run([
                str(self.build_dir / "test" / "test_pipeline_basic")
            ], capture_output=True, text=True, timeout=600)
            
            self.results["pipeline_integration"] = {
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr, 
                "passed": result.returncode == 0
            }
            
            if result.returncode == 0:
                print("✓ Pipeline integration tests PASSED")
            else:
                print("✗ Pipeline integration tests FAILED")
                print(f"Error: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            print("✗ Pipeline integration tests TIMEOUT")
            self.results["pipeline_integration"] = {"passed": False, "error": "timeout"}
        except FileNotFoundError:
            print("✗ Pipeline test executable NOT FOUND")
            self.results["pipeline_integration"] = {"passed": False, "error": "not_found"}
    
    def collect_metrics(self):
        """收集性能指标"""
        print("\n=== Collecting Performance Metrics ===")
        
        metrics_dir = self.build_dir / "metrics"
        if metrics_dir.exists():
            metrics_files = list(metrics_dir.glob("*.tsv"))
            print(f"Found {len(metrics_files)} metrics files")
            
            for metrics_file in metrics_files:
                print(f"Metrics file: {metrics_file}")
                if metrics_file.stat().st_size > 0:
                    with open(metrics_file, 'r') as f:
                        content = f.read()
                        print(f"Content preview:\n{content[:200]}...")
        else:
            print("No metrics directory found")
    
    def generate_report(self):
        """生成测试报告"""
        print("\n=== Test Summary Report ===")
        
        total_tests = len(self.results)
        passed_tests = sum(1 for r in self.results.values() if r.get("passed", False))
        
        print(f"Total Tests: {total_tests}")
        print(f"Passed: {passed_tests}")
        print(f"Failed: {total_tests - passed_tests}")
        print(f"Success Rate: {passed_tests/total_tests*100:.1f}%" if total_tests > 0 else "No tests run")
        
        print("\nDetailed Results:")
        for test_name, result in self.results.items():
            status = "PASS" if result.get("passed", False) else "FAIL"
            print(f"  {test_name}: {status}")
            if not result.get("passed", False) and "error" in result:
                print(f"    Error: {result['error']}")
        
        # 生成JSON报告
        report_path = self.build_dir / "test_report.json"
        with open(report_path, 'w') as f:
            json.dump({
                "timestamp": time.time(),
                "summary": {
                    "total": total_tests,
                    "passed": passed_tests,
                    "failed": total_tests - passed_tests
                },
                "results": self.results
            }, f, indent=2)
            
        print(f"\nDetailed report saved to: {report_path}")
        
        return passed_tests == total_tests
    
    def run_all(self):
        """运行所有测试（按优先级顺序）"""
        print("Starting Join Operator Test Suite")
        print("Priority Order: 1) Join Methods Correctness 2) Performance 3) Multi-thread Pipeline")
        
        # 高优先级测试
        self.run_unit_tests()       # Priority 1: Join方法正确性
        self.run_performance_tests() # Priority 2: Join算子性能测试  
        self.run_integration_tests() # Priority 3: 多线程Pipeline流程测试
        
        self.collect_metrics()
        return self.generate_report()

def main():
    parser = argparse.ArgumentParser(description="Run Join Operator Test Suite")
    parser.add_argument("--build-dir", default="build", help="Build directory")
    parser.add_argument("--config-dir", default="config", help="Config directory")
    parser.add_argument("--unit-only", action="store_true", help="Run only unit tests")
    parser.add_argument("--perf-only", action="store_true", help="Run only performance tests")
    parser.add_argument("--integration-only", action="store_true", help="Run only integration tests")
    
    args = parser.parse_args()
    
    runner = JoinTestRunner(args.build_dir, args.config_dir)
    
    if args.unit_only:
        runner.run_unit_tests()
    elif args.perf_only:
        runner.run_performance_tests()
    elif args.integration_only:
        runner.run_integration_tests()
    else:
        success = runner.run_all()
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()