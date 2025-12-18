#!/usr/bin/env python3
"""
SageFlow 集成测试运行脚本

提供命令行接口运行集成测试，支持选择特定的 Join 方法、并行度和数据规模。
测试完成后可自动生成可视化图表。

使用示例:
  # 测试 BruteForce 方法
  python scripts/run_integration_test.py --methods bruteforce
  
  # 测试多个方法
  python scripts/run_integration_test.py --methods bruteforce ivf hdr_tree
  
  # 测试所有方法，并生成可视化
  python scripts/run_integration_test.py --methods all --parallelism 1 2 4 --visualize
  
  # 指定数据规模
  python scripts/run_integration_test.py --methods bruteforce --data-sizes 500 1000 2000

Author: SageFlow Team
Date: 2025-12-15
"""

import argparse
import subprocess
import json
import os
import sys
import time
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime


# ============================================================================
# 常量定义
# ============================================================================

# 方法名到 gtest_filter 的映射
METHOD_FILTER_MAP = {
    'bruteforce': '*bruteforce*',
    'ivf': '*ivf*',
    'hnsw': '*hnsw*',
    'hdr_tree': '*hdr_tree*',
    'clustered_join': '*clustered_join*',
    's3j': '*s3j*',
    'vsjoin': '*vsjoin*',
    'faiss': '*faiss*',
}

# 支持的所有方法
SUPPORTED_METHODS = list(METHOD_FILTER_MAP.keys()) + ['all']

# 默认配置
DEFAULT_BINARY_PATH = 'build/bin/test_join_baseline_integration'
DEFAULT_CONFIG_PATH = 'config/integration_test_cases.toml'
DEFAULT_OUTPUT_DIR = 'test/result/integration'


# ============================================================================
# 参数解析
# ============================================================================

def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='Run SageFlow integration tests with specific Join methods',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --methods bruteforce ivf
  %(prog)s --methods all --parallelism 1 2 4 8 --visualize
  %(prog)s --methods hdr_tree --data-sizes 1000 2000 --output-dir results/
        """
    )
    
    # 测试选择
    parser.add_argument(
        '--methods', '-m',
        nargs='+',
        choices=SUPPORTED_METHODS,
        default=['bruteforce'],
        help='Join methods to test (default: bruteforce)'
    )
    
    parser.add_argument(
        '--parallelism', '-p',
        nargs='+',
        type=int,
        default=None,
        help='Parallelism levels to test (overrides config file)'
    )
    
    parser.add_argument(
        '--data-sizes', '-d',
        nargs='+',
        type=int,
        default=None,
        help='Data sizes to test (overrides config file)'
    )
    
    # 输出配置
    parser.add_argument(
        '--output-dir', '-o',
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help=f'Output directory for results (default: {DEFAULT_OUTPUT_DIR})'
    )
    
    parser.add_argument(
        '--config', '-c',
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help=f'Test configuration file (default: {DEFAULT_CONFIG_PATH})'
    )
    
    # 二进制和构建
    parser.add_argument(
        '--binary-path', '-b',
        type=str,
        default=DEFAULT_BINARY_PATH,
        help=f'Path to test binary (default: {DEFAULT_BINARY_PATH})'
    )
    
    parser.add_argument(
        '--build', 
        action='store_true',
        help='Build the test binary before running'
    )
    
    parser.add_argument(
        '--build-type',
        type=str,
        default='Release',
        choices=['Debug', 'Release', 'RelWithDebInfo'],
        help='Build type (default: Release)'
    )
    
    # 可视化
    parser.add_argument(
        '--visualize', '-v',
        action='store_true',
        help='Generate visualization charts after tests'
    )
    
    parser.add_argument(
        '--chart-format',
        type=str,
        default='png',
        choices=['png', 'svg', 'pdf'],
        help='Output format for charts (default: png)'
    )
    
    # 其他选项
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Print commands without executing'
    )
    
    parser.add_argument(
        '--timeout',
        type=int,
        default=3600,
        help='Timeout in seconds (default: 3600)'
    )
    
    return parser.parse_args()


# ============================================================================
# 构建功能
# ============================================================================

def build_project(build_type: str = 'Release', verbose: bool = False) -> bool:
    """构建项目
    
    Args:
        build_type: 构建类型 (Debug/Release/RelWithDebInfo)
        verbose: 是否显示详细输出
        
    Returns:
        构建是否成功
    """
    print(f"\n{'='*60}")
    print(f"Building project ({build_type})...")
    print(f"{'='*60}")
    
    # 创建 build 目录
    build_dir = Path('build')
    build_dir.mkdir(exist_ok=True)
    
    # CMake 配置
    cmake_cmd = [
        'cmake', '-B', 'build',
        f'-DCMAKE_BUILD_TYPE={build_type}',
        '-DBUILD_TESTING=ON',
        '-DSAGEFLOW_ENABLE_METRICS=ON'
    ]
    
    print(f"Running: {' '.join(cmake_cmd)}")
    result = subprocess.run(
        cmake_cmd,
        capture_output=not verbose,
        text=True
    )
    
    if result.returncode != 0:
        print(f"CMake configuration failed!")
        if not verbose and result.stderr:
            print(result.stderr)
        return False
    
    # 构建
    # 获取 CPU 核心数
    try:
        import multiprocessing
        jobs = multiprocessing.cpu_count()
    except:
        jobs = 4
    
    build_cmd = ['cmake', '--build', 'build', '-j', str(jobs)]
    
    print(f"Running: {' '.join(build_cmd)}")
    result = subprocess.run(
        build_cmd,
        capture_output=not verbose,
        text=True
    )
    
    if result.returncode != 0:
        print(f"Build failed!")
        if not verbose and result.stderr:
            print(result.stderr[-2000:] if len(result.stderr) > 2000 else result.stderr)
        return False
    
    print("Build successful!")
    return True


# ============================================================================
# 测试运行
# ============================================================================

def build_gtest_filter(methods: List[str]) -> str:
    """构建 gtest_filter 字符串
    
    Args:
        methods: 要测试的方法列表
        
    Returns:
        gtest_filter 字符串
    """
    if 'all' in methods:
        return ''  # 不过滤，运行全部
    
    filters = []
    for method in methods:
        if method in METHOD_FILTER_MAP:
            filters.append(METHOD_FILTER_MAP[method])
    
    return ':'.join(filters) if filters else ''


def run_test_binary(
    binary_path: str,
    gtest_filter: str,
    output_dir: str,
    timeout: int = 3600,
    verbose: bool = False,
    dry_run: bool = False
) -> Tuple[bool, str, str]:
    """运行测试二进制
    
    Args:
        binary_path: 测试二进制路径
        gtest_filter: gtest 过滤器字符串
        output_dir: 输出目录
        timeout: 超时时间（秒）
        verbose: 是否显示详细输出
        dry_run: 是否仅打印命令
        
    Returns:
        (成功标志, stdout, stderr)
    """
    # 检查二进制是否存在
    if not Path(binary_path).exists():
        return False, '', f"Binary not found: {binary_path}"
    
    # 构建命令
    cmd = [binary_path]
    if gtest_filter:
        cmd.append(f'--gtest_filter={gtest_filter}')
    
    # 设置环境变量
    env = os.environ.copy()
    env['SAGEFLOW_TEST_OUTPUT_DIR'] = output_dir
    
    # 确保输出目录存在
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"Running tests...")
    print(f"{'='*60}")
    print(f"Binary: {binary_path}")
    if gtest_filter:
        print(f"Filter: {gtest_filter}")
    print(f"Output: {output_dir}")
    print(f"Command: {' '.join(cmd)}")
    
    if dry_run:
        print("\n[DRY RUN] Would execute the above command")
        return True, '', ''
    
    # 运行测试
    start_time = time.time()
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env
        )
        elapsed = time.time() - start_time
        
        # 输出结果
        if verbose or result.returncode != 0:
            if result.stdout:
                print("\n--- STDOUT ---")
                print(result.stdout[-5000:] if len(result.stdout) > 5000 else result.stdout)
            if result.stderr:
                print("\n--- STDERR ---")
                print(result.stderr[-2000:] if len(result.stderr) > 2000 else result.stderr)
        
        print(f"\nTests completed in {elapsed:.1f}s")
        
        if result.returncode == 0:
            print("All tests PASSED!")
        else:
            print(f"Some tests FAILED (return code: {result.returncode})")
        
        return result.returncode == 0, result.stdout, result.stderr
        
    except subprocess.TimeoutExpired:
        print(f"\nTest execution timed out after {timeout}s!")
        return False, '', 'Timeout'
    except Exception as e:
        print(f"\nError running tests: {e}")
        return False, '', str(e)


def collect_results(output_dir: str) -> Dict:
    """收集测试结果
    
    Args:
        output_dir: 输出目录
        
    Returns:
        合并的结果字典
    """
    results = {}
    output_path = Path(output_dir)
    
    # 查找所有 JSON 报告
    for json_file in output_path.glob('*report*.json'):
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
                key = json_file.stem.replace('_report', '').replace('report', 'main')
                results[key] = data
        except json.JSONDecodeError as e:
            print(f"Warning: Failed to parse {json_file}: {e}")
        except Exception as e:
            print(f"Warning: Error reading {json_file}: {e}")
    
    return results


def print_results_summary(results: Dict) -> None:
    """打印结果摘要
    
    Args:
        results: 测试结果字典
    """
    print(f"\n{'='*60}")
    print("Results Summary")
    print(f"{'='*60}")
    
    for key, data in results.items():
        if 'summary' in data:
            summary = data['summary']
            print(f"\n{key}:")
            print(f"  Total:  {summary.get('total_tests', 0)}")
            print(f"  Passed: {summary.get('passed', 0)}")
            print(f"  Failed: {summary.get('failed', 0)}")
            print(f"  Skipped: {summary.get('skipped', 0)}")
        
        if 'algorithm_results' in data:
            print("\n  Algorithm Performance:")
            for algo, stats in data['algorithm_results'].items():
                avg_recall = stats.get('avg_recall', 0)
                avg_throughput = stats.get('avg_throughput', 0)
                print(f"    {algo}: recall={avg_recall:.4f}, throughput={avg_throughput:.1f} rec/s")


# ============================================================================
# 主函数
# ============================================================================

def main():
    """主函数"""
    args = parse_args()
    
    # 打印配置
    print(f"\n{'='*60}")
    print("SageFlow Integration Test Runner")
    print(f"{'='*60}")
    print(f"Methods: {args.methods}")
    if args.parallelism:
        print(f"Parallelism: {args.parallelism}")
    if args.data_sizes:
        print(f"Data sizes: {args.data_sizes}")
    print(f"Output directory: {args.output_dir}")
    print(f"Binary path: {args.binary_path}")
    print(f"Visualize: {args.visualize}")
    
    # 构建（如果需要）
    if args.build:
        if not build_project(args.build_type, args.verbose):
            print("Build failed, exiting.")
            return 1
    
    # 构建 gtest_filter
    gtest_filter = build_gtest_filter(args.methods)
    
    # 运行测试
    success, stdout, stderr = run_test_binary(
        binary_path=args.binary_path,
        gtest_filter=gtest_filter,
        output_dir=args.output_dir,
        timeout=args.timeout,
        verbose=args.verbose,
        dry_run=args.dry_run
    )
    
    if args.dry_run:
        return 0
    
    # 收集结果
    results = collect_results(args.output_dir)
    
    if results:
        print_results_summary(results)
    else:
        print("\nNo result files found.")
    
    # 生成可视化
    if args.visualize and results:
        print(f"\n{'='*60}")
        print("Generating visualization...")
        print(f"{'='*60}")
        
        try:
            # 动态导入可视化模块
            script_dir = Path(__file__).parent
            sys.path.insert(0, str(script_dir))
            
            from visualize_results import generate_charts
            generate_charts(args.output_dir, args.output_dir, args.chart_format)
            
        except ImportError as e:
            print(f"Warning: Could not import visualization module: {e}")
            print("Make sure matplotlib is installed: pip install matplotlib numpy")
        except Exception as e:
            print(f"Warning: Visualization failed: {e}")
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
