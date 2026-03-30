#!/usr/bin/env python3
"""
SageFlow 集成测试运行脚本

提供命令行接口运行集成测试，支持选择特定的 Join 方法、并行度和数据规模。
会根据参数生成临时配置文件（保存在输出目录下便于调试），然后运行测试。
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
  
  # 指定并行度（会创建临时配置文件，只保留指定的并行度）
  python scripts/run_integration_test.py --methods bruteforce --parallelism 2 4 8
  
  # 使用 gtest-filter 精确匹配测试用例（支持通配符 * 和 ?）
  python scripts/run_integration_test.py --gtest-filter "*exp_a*r005*" --parallelism 2 4 8
  
  # ClusteredJoin 说明：num_partitions 会在运行时被强制设置为 parallelism
  # 配置文件中的 num_partitions 会被忽略，直接使用 --parallelism 指定的值
  python scripts/run_integration_test.py --methods clustered_join --parallelism 2 4 -c config/clustered_experiment.toml

临时配置文件位置: <output-dir>/run_<timestamp>/filtered_config.toml

Author: SageFlow Team
Date: 2025-12-15
"""

import argparse
import subprocess
import json
import os
import sys
import time
import re
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime

try:
    import tomllib  # Python 3.11+
except ImportError:
    try:
        import tomli as tomllib  # fallback for older Python
    except ImportError:
        tomllib = None


# ============================================================================
# 常量定义
# ============================================================================

# 方法名到 gtest_filter 的映射
METHOD_FILTER_MAP = {
    'bruteforce': '*bruteforce*',
    'ivf': '*ivf*',
    'hnsw': '*hnsw*',
    'hdr_tree': '*hdr_tree*',
    'clustered_join': '*clustered*',
    's3j': '*s3j*',
    'vsjoin': '*vsjoin*',
}

# 支持的所有方法
SUPPORTED_METHODS = list(METHOD_FILTER_MAP.keys()) + ['all']

# 默认配置
DEFAULT_BINARY_PATH = 'build/bin/test_join_baseline_integration'
DEFAULT_CONFIG_PATH = 'config/integration_test_cases.toml'
DEFAULT_OUTPUT_DIR = 'test/result/integration'

# 算法名称到 test_case.algorithm 字段的映射
ALGORITHM_NAME_MAP = {
    'bruteforce': ['bruteforce'],
    'ivf': ['ivf'],
    'hnsw': ['hnsw'],
    'hdr_tree': ['hdr_tree'],
    'clustered_join': ['clustered_join'],
    's3j': ['s3j'],
    'vsjoin': ['vsjoin'],
}


# ============================================================================
# TOML 配置处理
# ============================================================================

def load_toml_config(config_path: str) -> Dict[str, Any]:
    """读取 TOML 配置文件
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        配置字典
    """
    if tomllib is None:
        raise ImportError(
            "需要 tomllib (Python 3.11+) 或 tomli 来解析 TOML 文件。\n"
            "请运行: pip install tomli"
        )
    
    with open(config_path, 'rb') as f:
        return tomllib.load(f)


def filter_test_cases(
    config: Dict[str, Any],
    methods: List[str],
    gtest_filter: Optional[str] = None
) -> List[Dict[str, Any]]:
    """根据方法和过滤器筛选测试用例
    
    Args:
        config: 原始配置字典
        methods: 要测试的方法列表
        gtest_filter: 可选的 gtest filter 字符串
        
    Returns:
        筛选后的测试用例列表
    """
    test_cases = config.get('test_case', [])
    
    # 如果是 'all'，返回所有测试用例
    if 'all' in methods and not gtest_filter:
        return test_cases
    
    filtered = []
    
    # 收集目标算法名称
    target_algorithms = set()
    if 'all' not in methods:
        for method in methods:
            if method in ALGORITHM_NAME_MAP:
                target_algorithms.update(ALGORITHM_NAME_MAP[method])
    
    for tc in test_cases:
        tc_name = tc.get('name', '')
        tc_algorithm = tc.get('algorithm', '')
        
        # 检查是否启用
        if not tc.get('enabled', True):
            continue
        
        # 如果指定了 gtest_filter，用它来过滤
        if gtest_filter:
            # 解析 gtest_filter 格式: "*pattern1*:*pattern2*"
            patterns = gtest_filter.split(':')
            matched = False
            for pattern in patterns:
                # 将 gtest 通配符模式转换为正则表达式
                # * 匹配任意字符（包括空）
                # ? 匹配单个字符
                regex_pattern = pattern.replace('.', r'\.').replace('*', '.*').replace('?', '.')
                if re.match(f'^{regex_pattern}$', tc_name):
                    matched = True
                    break
            if matched:
                filtered.append(tc)
            continue
        
        # 否则按算法名过滤
        if 'all' in methods or tc_algorithm in target_algorithms:
            filtered.append(tc)
    
    return filtered


def modify_test_cases(
    test_cases: List[Dict[str, Any]],
    parallelism: Optional[List[int]] = None,
    data_sizes: Optional[List[int]] = None
) -> List[Dict[str, Any]]:
    """修改测试用例的参数
    
    Args:
        test_cases: 测试用例列表
        parallelism: 并行度列表（如果指定，则覆盖）
        data_sizes: 数据规模列表（如果指定，则覆盖）
        
    Returns:
        修改后的测试用例列表
    """
    modified = []
    
    for tc in test_cases:
        tc_copy = dict(tc)
        
        if parallelism is not None:
            # 直接覆盖 parallelism
            # 注意：ClusteredJoin 的 num_partitions 会在运行时被强制设置为 parallelism
            # 参见 src/operator/join_operator.cpp 中的 "runtime constraint auto-fix"
            tc_copy['parallelism'] = list(parallelism)
        
        if data_sizes is not None:
            tc_copy['data_sizes'] = list(data_sizes)
        
        modified.append(tc_copy)
    
    return modified


def format_toml_value(value: Any) -> str:
    """将 Python 值转换为 TOML 格式字符串
    
    Args:
        value: Python 值
        
    Returns:
        TOML 格式字符串
    """
    if isinstance(value, bool):
        return 'true' if value else 'false'
    elif isinstance(value, str):
        # 转义引号
        escaped = value.replace('\\', '\\\\').replace('"', '\\"')
        return f'"{escaped}"'
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, list):
        items = [format_toml_value(item) for item in value]
        return '[' + ', '.join(items) + ']'
    else:
        return str(value)


def generate_temp_toml(
    config: Dict[str, Any],
    test_cases: List[Dict[str, Any]],
    output_path: Path,
    original_config_path: str,
    args: argparse.Namespace
) -> Path:
    """生成临时 TOML 配置文件
    
    Args:
        config: 原始配置字典
        test_cases: 筛选和修改后的测试用例列表
        output_path: 输出目录
        original_config_path: 原始配置文件路径
        args: 命令行参数
        
    Returns:
        临时配置文件路径
    """
    temp_path = output_path / "filtered_config.toml"
    
    with open(temp_path, 'w', encoding='utf-8') as f:
        # 写入注释头
        f.write("# Auto-generated filtered configuration\n")
        f.write(f"# Generated at: {datetime.now().isoformat()}\n")
        f.write(f"# Original config: {original_config_path}\n")
        f.write(f"# Methods filter: {args.methods}\n")
        if args.gtest_filter:
            f.write(f"# GTest filter: {args.gtest_filter}\n")
        if args.parallelism:
            f.write(f"# Parallelism override: {args.parallelism}\n")
        if args.data_sizes:
            f.write(f"# Data sizes override: {args.data_sizes}\n")
        f.write(f"# Total test cases: {len(test_cases)}\n")
        f.write("\n")
        
        # 写入 [common] 节
        if 'common' in config:
            f.write("# ==================== 通用配置 ====================\n")
            f.write("[common]\n")
            common = dict(config['common'])
            # 更新 result_output_dir 为当前输出目录
            common['result_output_dir'] = str(output_path)
            for key, value in common.items():
                f.write(f"{key} = {format_toml_value(value)}\n")
            f.write("\n")
        
        # 写入测试用例
        if test_cases:
            f.write("# ==================== 测试用例 ====================\n")
            for tc in test_cases:
                f.write(f"\n[[test_case]]\n")
                for key, value in tc.items():
                    f.write(f"{key} = {format_toml_value(value)}\n")
    
    return temp_path


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

    # 允许直接指定 gtest_filter（高级用法），用于只跑特定 test_case（例如 exp_a_p2_r005）
    # 注意：如果提供该参数，将覆盖 --methods 生成的 filter。
    parser.add_argument(
        '--gtest-filter',
        type=str,
        default=None,
        help='Override gtest filter string (e.g. "*exp_a_p2_r005*" or "*exp_b_k8*:*exp_b_k12*")'
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
    config_path: str = '',
    timeout: int = 3600,
    verbose: bool = False,
    dry_run: bool = False,
    log_file: Optional[Path] = None
) -> Tuple[bool, str, str]:
    """运行测试二进制
    
    Args:
        binary_path: 测试二进制路径
        gtest_filter: gtest 过滤器字符串
        output_dir: 输出目录
        config_path: 测试配置文件路径（如果为空则使用默认配置）
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
    
    # 设置配置文件路径环境变量
    if config_path:
        env['SAGEFLOW_TEST_CONFIG_PATH'] = config_path
    
    # 确保输出目录存在
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"Running tests...")
    print(f"{'='*60}")
    print(f"Binary: {binary_path}")
    if config_path:
        print(f"Config: {config_path}")
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

        if log_file is not None:
            try:
                log_file.parent.mkdir(parents=True, exist_ok=True)
                with open(log_file, 'w', encoding='utf-8') as f:
                    f.write(f"# SageFlow Integration Test Runner Log\n")
                    f.write(f"started_at={datetime.now().isoformat()}\n")
                    f.write(f"finished_at={datetime.now().isoformat()}\n")
                    f.write(f"binary={binary_path}\n")
                    if config_path:
                        f.write(f"config={config_path}\n")
                    if gtest_filter:
                        f.write(f"gtest_filter={gtest_filter}\n")
                    f.write(f"output_dir={output_dir}\n")
                    f.write(f"command={' '.join(cmd)}\n")
                    f.write("\n===== STDOUT =====\n")
                    f.write(result.stdout or "")
                    f.write("\n\n===== STDERR =====\n")
                    f.write(result.stderr or "")
            except Exception as e:
                print(f"Warning: failed to write runner log to {log_file}: {e}")
        elapsed = time.time() - start_time
        
        # 输出结果
        if verbose or result.returncode != 0:
            if result.stdout:
                print("\n--- STDOUT ---")
                print(result.stdout[-5000:] if len(result.stdout) > 5000 else result.stdout)
            if result.stderr:
                print("\n--- STDERR ---")
                print(result.stderr[-2000:] if len(result.stderr) > 2000 else result.stderr)

        # 记录底层日志（B 类：底层二进制 stdout/stderr）
        if log_file is not None:
            try:
                log_file.parent.mkdir(parents=True, exist_ok=True)
                with open(log_file, 'w', encoding='utf-8') as f:
                    f.write(f"# SageFlow Integration Test Binary Log\n")
                    f.write(f"started_at={datetime.now().isoformat()}\n")
                    f.write(f"binary={binary_path}\n")
                    if config_path:
                        f.write(f"config={config_path}\n")
                    if gtest_filter:
                        f.write(f"gtest_filter={gtest_filter}\n")
                    f.write(f"output_dir={output_dir}\n")
                    f.write(f"command={' '.join(cmd)}\n")
                    f.write(f"returncode={result.returncode}\n")
                    f.write("\n===== STDOUT =====\n")
                    f.write(result.stdout or "")
                    f.write("\n\n===== STDERR =====\n")
                    f.write(result.stderr or "")
            except Exception as e:
                print(f"Warning: failed to write binary log to {log_file}: {e}")
        
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

    # 兼容：测试二进制当前会将汇总报告写入固定目录 test/result/integration/
    # 即使 per-test CSV 写到了 result_output_dir（由 SAGEFLOW_TEST_OUTPUT_DIR 覆盖）。
    if not results:
        fallback_path = Path('test/result/integration')
        for json_file in fallback_path.glob('*report*.json'):
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    key = json_file.stem.replace('_report', '').replace('report', 'main')
                    results[key] = data
            except json.JSONDecodeError:
                continue
            except Exception:
                continue
    
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
    print(f"Config: {args.config}")
    if args.parallelism:
        print(f"Parallelism: {args.parallelism}")
    if args.data_sizes:
        print(f"Data sizes: {args.data_sizes}")
    print(f"Output directory: {args.output_dir} (will create per-run subfolder)")
    print(f"Binary path: {args.binary_path}")
    print(f"Visualize: {args.visualize}")
    
    # 构建（如果需要）
    if args.build:
        if not build_project(args.build_type, args.verbose):
            print("Build failed, exiting.")
            return 1
    
    # 为本次运行创建独立输出目录，避免与历史产物混用
    run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    run_dir = Path(args.output_dir) / f"run_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    
    # 检查是否需要生成临时配置文件
    # 条件：指定了 --parallelism、--data-sizes、--methods（非 all）或 --gtest-filter
    needs_temp_config = (
        args.parallelism is not None or 
        args.data_sizes is not None or
        ('all' not in args.methods) or
        args.gtest_filter is not None
    )
    
    # 构建 gtest_filter（用于过滤测试用例，如果要用临时配置，则也用于过滤 TOML）
    gtest_filter = args.gtest_filter if args.gtest_filter else build_gtest_filter(args.methods)
    
    # 确定最终使用的配置文件
    config_path_to_use = args.config
    
    if needs_temp_config:
        try:
            print(f"\n{'='*60}")
            print("Generating filtered configuration...")
            print(f"{'='*60}")
            
            # 加载原始配置
            config = load_toml_config(args.config)
            print(f"Loaded config: {args.config}")
            original_test_count = len(config.get('test_case', []))
            print(f"Original test cases: {original_test_count}")
            
            # 筛选测试用例
            filtered_cases = filter_test_cases(config, args.methods, args.gtest_filter)
            print(f"After method/filter: {len(filtered_cases)} test cases")
            
            if not filtered_cases:
                print("Warning: No test cases match the specified filters!")
                print("Check your --methods or --gtest-filter arguments.")
                # 继续执行，让 gtest 本身来报告没有匹配的测试
            
            # 修改测试用例参数
            modified_cases = modify_test_cases(
                filtered_cases,
                parallelism=args.parallelism,
                data_sizes=args.data_sizes
            )
            
            # 生成临时配置文件
            temp_config_path = generate_temp_toml(
                config,
                modified_cases,
                run_dir,
                args.config,
                args
            )
            config_path_to_use = str(temp_config_path)
            print(f"Generated temp config: {temp_config_path}")
            
            # 打印测试用例摘要
            if args.verbose and modified_cases:
                print("\nFiltered test cases:")
                for tc in modified_cases:
                    name = tc.get('name', 'unknown')
                    algo = tc.get('algorithm', 'unknown')
                    parallelism = tc.get('parallelism', [])
                    data_sizes = tc.get('data_sizes', [])
                    print(f"  - {name}: algorithm={algo}, parallelism={parallelism}, data_sizes={data_sizes}")
            
            # 由于我们已经在 TOML 层面做了过滤，gtest_filter 可以留空或保持
            # 这样做是为了让 C++ 侧也能看到完整的测试名称进行过滤
            # 但由于临时 TOML 已经只包含需要的测试用例，实际上不需要再过滤
            # 不过保留 gtest_filter 可以提供额外的安全保障
            
        except ImportError as e:
            print(f"Warning: Cannot generate temp config - {e}")
            print("Falling back to original config with gtest_filter only.")
            config_path_to_use = args.config
        except FileNotFoundError as e:
            print(f"Error: Config file not found - {e}")
            return 1
        except Exception as e:
            print(f"Warning: Failed to generate temp config - {e}")
            print("Falling back to original config with gtest_filter only.")
            config_path_to_use = args.config

    # 运行测试（落盘 runner 日志：记录脚本视角的 stdout/stderr）
    # A 类日志：脚本层日志（记录本脚本视角的关键信息 + 后续汇总信息）
    runner_log = run_dir / "logs" / "runner.log"
    runner_log.parent.mkdir(parents=True, exist_ok=True)
    with open(runner_log, 'w', encoding='utf-8') as f:
        f.write("# SageFlow Integration Test Runner Log\n")
        f.write(f"started_at={datetime.now().isoformat()}\n")
        f.write(f"methods={args.methods}\n")
        f.write(f"original_config={args.config}\n")
        f.write(f"effective_config={config_path_to_use}\n")
        f.write(f"output_dir={str(run_dir)}\n")
        f.write(f"binary_path={args.binary_path}\n")
        f.write(f"visualize={args.visualize}\n")
        f.write(f"gtest_filter={gtest_filter}\n")
        if args.parallelism:
            f.write(f"parallelism_override={args.parallelism}\n")
        if args.data_sizes:
            f.write(f"data_sizes_override={args.data_sizes}\n")

    # B 类日志：底层二进制 stdout/stderr
    binary_log = run_dir / "logs" / "binary.log"

    success, stdout, stderr = run_test_binary(
        binary_path=args.binary_path,
        gtest_filter=gtest_filter,
        output_dir=str(run_dir),
        config_path=config_path_to_use,
        timeout=args.timeout,
        verbose=args.verbose,
        dry_run=args.dry_run,
        log_file=binary_log
    )
    
    if args.dry_run:
        return 0
    
    # 收集结果
    results = collect_results(str(run_dir))
    
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
            generate_charts(str(run_dir), str(run_dir), args.chart_format)
            
        except ImportError as e:
            print(f"Warning: Could not import visualization module: {e}")
            print("Make sure matplotlib is installed: pip install matplotlib numpy")
        except Exception as e:
            print(f"Warning: Visualization failed: {e}")
    
    return 0 if success else 1
                    

if __name__ == '__main__':
    sys.exit(main())
