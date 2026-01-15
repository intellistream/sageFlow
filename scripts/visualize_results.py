#!/usr/bin/env python3
"""
SageFlow 测试结果可视化模块

生成多种类型的可视化图表：
  1. 耗时 Breakdown 堆叠柱状图 - 分析各阶段耗时占比
  2. 算法吞吐量对比图 - 比较不同算法在不同并行度下的吞吐量
  3. 算法召回率对比图 - 比较不同算法的召回率
  4. 并行度扩展性图 - 分析吞吐量随并行度变化的扩展性
  5. 端到端延迟分位数图 - P95/P99 延迟对比

使用示例:
  python scripts/visualize_results.py --input-dir test/result/integration
  python scripts/visualize_results.py --input-dir test/result/integration --format svg
  python scripts/visualize_results.py --input-dir test/result/integration --charts breakdown throughput

Author: SageFlow Team
Date: 2025-12-15
"""

import json
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict

# 延迟导入 matplotlib，以便在没有安装时提供友好的错误信息
try:
    import matplotlib
    matplotlib.use('Agg')  # 使用非交互式后端
    import matplotlib.pyplot as plt
    import numpy as np
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("Warning: matplotlib not installed. Install with: pip install matplotlib numpy")


# ============================================================================
# 配置和常量
# ============================================================================

# 设置字体，避免中文问题
if MATPLOTLIB_AVAILABLE:
    plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial', 'Helvetica', 'sans-serif']
    plt.rcParams['axes.unicode_minus'] = False
    plt.rcParams['figure.dpi'] = 100
    plt.rcParams['savefig.dpi'] = 150
    plt.rcParams['figure.figsize'] = (12, 8)

# Breakdown 阶段颜色映射（从底部到顶部的堆叠顺序）
BREAKDOWN_COLORS = {
    'window_insert': '#4CAF50',     # 绿色
    'index_insert': '#2196F3',      # 蓝色
    'expire': '#9C27B0',            # 紫色
    'candidate_fetch': '#FF9800',   # 橙色
    'similarity': '#F44336',        # 红色
    'join_function': '#00BCD4',     # 青色
    'emit': '#795548',              # 棕色
    'lock_wait': '#607D8B',         # 灰色
}

# Breakdown 阶段的显示标签
BREAKDOWN_LABELS = {
    'window_insert': 'Window Insert',
    'index_insert': 'Index Insert',
    'expire': 'Expire',
    'candidate_fetch': 'Candidate Fetch',
    'similarity': 'Similarity Calc',
    'join_function': 'Join Function',
    'emit': 'Emit',
    'lock_wait': 'Lock Wait',
}

# 算法颜色映射
ALGORITHM_COLORS = {
    'bruteforce': '#E53935',     # 红色
    'ivf': '#1E88E5',            # 蓝色
    'hnsw': '#43A047',           # 绿色
    'hdr_tree': '#FB8C00',       # 橙色
    'clustered_join': '#8E24AA', # 紫色
    's3j': '#00ACC1',            # 青色
    'vsjoin': '#6D4C41',         # 棕色
}

# 算法标记样式
ALGORITHM_MARKERS = {
    'bruteforce': 'o',
    'ivf': 's',
    'hnsw': '^',
    'hdr_tree': 'D',
    'clustered_join': 'v',
    's3j': '<',
    'vsjoin': '>',
}


# ============================================================================
# 数据加载
# ============================================================================

def load_results(result_dir: str) -> Dict[str, Any]:
    """加载测试结果 JSON 文件
    
    Args:
        result_dir: 结果目录路径
        
    Returns:
        包含所有结果的字典，键为文件名（不含后缀）
    """
    results = {}
    result_path = Path(result_dir)
    
    if not result_path.exists():
        print(f"Warning: Result directory does not exist: {result_dir}")
        return results
    
    # 查找所有 JSON 报告文件
    json_files = list(result_path.glob('*report*.json')) + list(result_path.glob('*.json'))
    json_files = list(set(json_files))  # 去重
    
    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
                key = json_file.stem.replace('_report', '')
                results[key] = data
                print(f"Loaded: {json_file.name}")
        except json.JSONDecodeError as e:
            print(f"Warning: Failed to parse {json_file}: {e}")
        except Exception as e:
            print(f"Warning: Error reading {json_file}: {e}")
    
    return results


def extract_detailed_results(results: Dict[str, Any]) -> List[Dict]:
    """从多个报告中提取详细结果
    
    Args:
        results: load_results 返回的结果字典
        
    Returns:
        合并的详细结果列表
    """
    detailed = []
    
    for report_name, data in results.items():
        if 'detailed_results' in data:
            for result in data['detailed_results']:
                # 确保每个结果都有来源标识
                result['source_report'] = report_name
                detailed.append(result)
    
    return detailed


# ============================================================================
# Breakdown 堆叠柱状图
# ============================================================================

def generate_breakdown_chart(
    results: Dict[str, Any], 
    output_path: str,
    format: str = 'png'
) -> bool:
    """生成耗时 Breakdown 堆叠柱状图
    
    X轴: 算法名称 + 并行度
    Y轴: 耗时（毫秒）
    堆叠内容: 各个处理阶段
    
    Args:
        results: 测试结果字典
        output_path: 输出文件路径（不含扩展名）
        format: 输出格式 (png/svg/pdf)
        
    Returns:
        是否成功生成图表
    """
    if not MATPLOTLIB_AVAILABLE:
        return False
    
    detailed = extract_detailed_results(results)
    if not detailed:
        print("No detailed results found for breakdown chart")
        return False
    
    # 按算法和并行度分组，选择每组的第一个结果
    grouped = {}
    for result in detailed:
        algo = result.get('algorithm', 'unknown')
        para = result.get('parallelism', 1)
        key = (algo, para)
        if key not in grouped:
            grouped[key] = result
    
    # 按算法名排序，再按并行度排序
    sorted_keys = sorted(grouped.keys(), key=lambda x: (x[0], x[1]))
    
    if not sorted_keys:
        print("No valid results for breakdown chart")
        return False
    
    # 准备数据
    labels = []
    breakdown_data = {key: [] for key in BREAKDOWN_COLORS.keys()}
    
    for algo, para in sorted_keys:
        result = grouped[(algo, para)]
        breakdown = result.get('breakdown', {})
        
        label = f"{algo}\np={para}"
        labels.append(label)
        
        for key in BREAKDOWN_COLORS.keys():
            # 从纳秒转换为毫秒
            value_ns = breakdown.get(f'{key}_ns', 0)
            value_ms = value_ns / 1_000_000.0
            breakdown_data[key].append(value_ms)
    
    # 创建图表
    fig, ax = plt.subplots(figsize=(max(14, len(labels) * 1.2), 8))
    
    x = np.arange(len(labels))
    width = 0.6
    
    bottom = np.zeros(len(labels))
    
    # 绘制堆叠柱状图
    for key in BREAKDOWN_COLORS.keys():
        values = np.array(breakdown_data[key])
        if np.sum(values) > 0:  # 只绘制有数据的阶段
            bars = ax.bar(x, values, width, 
                         label=BREAKDOWN_LABELS[key], 
                         bottom=bottom, 
                         color=BREAKDOWN_COLORS[key],
                         edgecolor='white',
                         linewidth=0.5)
            bottom += values
    
    # 设置标签和标题
    ax.set_ylabel('Time (ms)', fontsize=12)
    ax.set_xlabel('Algorithm / Parallelism', fontsize=12)
    ax.set_title('Join Operator Breakdown Analysis', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right', fontsize=10)
    
    # 图例放在右侧
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), fontsize=10)
    
    # 添加网格
    ax.yaxis.grid(True, linestyle='--', alpha=0.3)
    ax.set_axisbelow(True)
    
    # 调整布局
    plt.tight_layout()
    
    # 保存图表
    full_path = f"{output_path}_breakdown.{format}"
    plt.savefig(full_path, format=format, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"Breakdown chart saved to: {full_path}")
    return True


# ============================================================================
# 吞吐量对比图
# ============================================================================

def generate_throughput_chart(
    results: Dict[str, Any], 
    output_path: str,
    format: str = 'png'
) -> bool:
    """生成算法吞吐量对比图
    
    X轴: 并行度
    Y轴: 吞吐量 (records/sec)
    多条折线: 不同算法
    
    Args:
        results: 测试结果字典
        output_path: 输出文件路径（不含扩展名）
        format: 输出格式
        
    Returns:
        是否成功生成图表
    """
    if not MATPLOTLIB_AVAILABLE:
        return False
    
    detailed = extract_detailed_results(results)
    if not detailed:
        print("No detailed results found for throughput chart")
        return False
    
    # 按算法分组
    algo_data = defaultdict(lambda: defaultdict(list))
    
    for result in detailed:
        algorithm = result.get('algorithm', 'unknown')
        parallelism = result.get('parallelism', 1)
        throughput = result.get('throughput_records_per_sec', 0)
        
        if throughput > 0:
            algo_data[algorithm][parallelism].append(throughput)
    
    if not algo_data:
        print("No throughput data found")
        return False
    
    # 创建图表
    fig, ax = plt.subplots(figsize=(12, 7))
    
    # 获取所有并行度
    all_parallelisms = sorted(set(
        p for d in algo_data.values() for p in d.keys()
    ))
    
    # 绘制每个算法的曲线
    for algorithm in sorted(algo_data.keys()):
        para_data = algo_data[algorithm]
        parallelisms = sorted(para_data.keys())
        throughputs = [np.mean(para_data[p]) for p in parallelisms]
        
        # 计算误差条（如果有多个数据点）
        yerr = [np.std(para_data[p]) if len(para_data[p]) > 1 else 0 
                for p in parallelisms]
        
        color = ALGORITHM_COLORS.get(algorithm, '#333333')
        marker = ALGORITHM_MARKERS.get(algorithm, 'o')
        
        ax.errorbar(parallelisms, throughputs,
                   yerr=yerr,
                   label=algorithm,
                   color=color,
                   marker=marker,
                   linewidth=2,
                   markersize=8,
                   capsize=4,
                   capthick=1.5)
    
    # 设置标签和标题
    ax.set_xlabel('Parallelism', fontsize=12)
    ax.set_ylabel('Throughput (records/sec)', fontsize=12)
    ax.set_title('Algorithm Throughput Comparison', fontsize=14, fontweight='bold')
    
    # 设置 X 轴刻度为整数
    ax.set_xticks(all_parallelisms)
    
    # 图例
    ax.legend(loc='best', fontsize=10)
    
    # 网格
    ax.grid(True, linestyle='--', alpha=0.3)
    ax.set_axisbelow(True)
    
    plt.tight_layout()
    
    # 保存
    full_path = f"{output_path}_throughput.{format}"
    plt.savefig(full_path, format=format, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"Throughput chart saved to: {full_path}")
    return True


# ============================================================================
# 召回率对比图
# ============================================================================

def generate_recall_comparison_chart(
    results: Dict[str, Any], 
    output_path: str,
    format: str = 'png'
) -> bool:
    """生成算法召回率对比图
    
    X轴: 算法
    Y轴: 召回率
    分组: 不同并行度
    
    Args:
        results: 测试结果字典
        output_path: 输出文件路径（不含扩展名）
        format: 输出格式
        
    Returns:
        是否成功生成图表
    """
    if not MATPLOTLIB_AVAILABLE:
        return False
    
    detailed = extract_detailed_results(results)
    if not detailed:
        print("No detailed results found for recall chart")
        return False
    
    # 按算法和并行度分组
    algo_para_recall = defaultdict(lambda: defaultdict(list))
    
    for result in detailed:
        algorithm = result.get('algorithm', 'unknown')
        parallelism = result.get('parallelism', 1)
        recall = result.get('recall', 0)
        
        algo_para_recall[algorithm][parallelism].append(recall)
    
    if not algo_para_recall:
        print("No recall data found")
        return False
    
    # 准备数据
    algorithms = sorted(algo_para_recall.keys())
    all_parallelisms = sorted(set(
        p for d in algo_para_recall.values() for p in d.keys()
    ))
    
    # 创建图表
    fig, ax = plt.subplots(figsize=(12, 7))
    
    x = np.arange(len(algorithms))
    width = 0.8 / max(len(all_parallelisms), 1)
    
    # 为每个并行度绘制一组柱子
    colors = plt.cm.viridis(np.linspace(0.2, 0.8, len(all_parallelisms)))
    
    for i, para in enumerate(all_parallelisms):
        recalls = []
        for algo in algorithms:
            if para in algo_para_recall.get(algo, {}):
                recalls.append(np.mean(algo_para_recall[algo][para]))
            else:
                recalls.append(0)
        
        offset = width * (i - len(all_parallelisms) / 2 + 0.5)
        ax.bar(x + offset, recalls, width, 
               label=f'p={para}', color=colors[i],
               edgecolor='white', linewidth=0.5)
    
    # 添加完美召回率参考线
    ax.axhline(y=1.0, color='r', linestyle='--', alpha=0.5, linewidth=2)
    ax.text(len(algorithms) - 0.5, 1.01, 'Perfect Recall', 
            color='r', alpha=0.7, fontsize=9, ha='right')
    
    # 设置标签和标题
    ax.set_xlabel('Algorithm', fontsize=12)
    ax.set_ylabel('Recall', fontsize=12)
    ax.set_title('Algorithm Recall Comparison by Parallelism', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(algorithms, fontsize=11)
    ax.set_ylim(0, 1.15)
    
    # 图例
    ax.legend(title='Parallelism', loc='lower right', fontsize=10)
    
    # 网格
    ax.yaxis.grid(True, linestyle='--', alpha=0.3)
    ax.set_axisbelow(True)
    
    plt.tight_layout()
    
    # 保存
    full_path = f"{output_path}_recall.{format}"
    plt.savefig(full_path, format=format, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"Recall comparison chart saved to: {full_path}")
    return True


# ============================================================================
# 扩展性图
# ============================================================================

def generate_scalability_chart(
    results: Dict[str, Any], 
    output_path: str,
    format: str = 'png'
) -> bool:
    """生成并行度扩展性图
    
    展示吞吐量随并行度增加的扩展性，包含理想线性扩展参考线
    
    Args:
        results: 测试结果字典
        output_path: 输出文件路径（不含扩展名）
        format: 输出格式
        
    Returns:
        是否成功生成图表
    """
    if not MATPLOTLIB_AVAILABLE:
        return False
    
    detailed = extract_detailed_results(results)
    if not detailed:
        print("No detailed results found for scalability chart")
        return False
    
    # 按算法分组
    algo_data = defaultdict(lambda: defaultdict(list))
    
    for result in detailed:
        algorithm = result.get('algorithm', 'unknown')
        parallelism = result.get('parallelism', 1)
        throughput = result.get('throughput_records_per_sec', 0)
        
        if throughput > 0:
            algo_data[algorithm][parallelism].append(throughput)
    
    if not algo_data:
        print("No throughput data found for scalability chart")
        return False
    
    # 创建图表
    fig, ax = plt.subplots(figsize=(12, 7))
    
    all_parallelisms = sorted(set(
        p for d in algo_data.values() for p in d.keys()
    ))
    
    if len(all_parallelisms) < 2:
        print("Need at least 2 parallelism levels for scalability chart")
        return False
    
    # 绘制每个算法的扩展性曲线（相对于 p=1 的加速比）
    for algorithm in sorted(algo_data.keys()):
        para_data = algo_data[algorithm]
        
        # 获取基准（p=1）吞吐量
        base_para = min(para_data.keys())
        base_throughput = np.mean(para_data[base_para])
        
        if base_throughput <= 0:
            continue
        
        parallelisms = sorted(para_data.keys())
        speedups = [np.mean(para_data[p]) / base_throughput for p in parallelisms]
        
        color = ALGORITHM_COLORS.get(algorithm, '#333333')
        marker = ALGORITHM_MARKERS.get(algorithm, 'o')
        
        ax.plot(parallelisms, speedups,
               label=algorithm,
               color=color,
               marker=marker,
               linewidth=2,
               markersize=8)
    
    # 绘制理想线性扩展参考线
    max_para = max(all_parallelisms)
    ideal_x = np.array([1, max_para])
    ideal_y = ideal_x  # 线性扩展
    ax.plot(ideal_x, ideal_y, 'k--', linewidth=1.5, alpha=0.5, label='Ideal Linear')
    
    # 设置标签和标题
    ax.set_xlabel('Parallelism', fontsize=12)
    ax.set_ylabel('Speedup (relative to p=1)', fontsize=12)
    ax.set_title('Parallelism Scalability Analysis', fontsize=14, fontweight='bold')
    
    # X 轴刻度
    ax.set_xticks(all_parallelisms)
    
    # 图例
    ax.legend(loc='upper left', fontsize=10)
    
    # 网格
    ax.grid(True, linestyle='--', alpha=0.3)
    ax.set_axisbelow(True)
    
    plt.tight_layout()
    
    # 保存
    full_path = f"{output_path}_scalability.{format}"
    plt.savefig(full_path, format=format, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"Scalability chart saved to: {full_path}")
    return True


# ============================================================================
# 端到端延迟分位数图
# ============================================================================

def generate_latency_percentile_chart(
    results: Dict[str, Any],
    output_path: str,
    format: str = 'png'
) -> bool:
    """生成端到端延迟分位数图（P95/P99）

    X轴: 算法名称 + 并行度
    Y轴: 延迟（毫秒）
    两组柱: P95 / P99
    """
    if not MATPLOTLIB_AVAILABLE:
        return False

    detailed = extract_detailed_results(results)
    if not detailed:
        print("No detailed results found for latency percentile chart")
        return False

    grouped = {}
    for result in detailed:
        algo = result.get('algorithm', 'unknown')
        para = result.get('parallelism', 1)
        breakdown = result.get('breakdown', {})
        p95 = breakdown.get('e2e_latency_p95_us', 0)
        p99 = breakdown.get('e2e_latency_p99_us', 0)
        if p95 > 0 or p99 > 0:
            grouped[(algo, para)] = (p95, p99)

    if not grouped:
        print("No latency percentile data found")
        return False

    sorted_keys = sorted(grouped.keys(), key=lambda x: (x[0], x[1]))
    labels = [f"{algo}\np={para}" for algo, para in sorted_keys]

    p95_ms = [grouped[k][0] / 1000.0 for k in sorted_keys]
    p99_ms = [grouped[k][1] / 1000.0 for k in sorted_keys]

    x = np.arange(len(labels))
    width = 0.35

    fig, ax = plt.subplots(figsize=(max(14, len(labels) * 1.2), 8))
    ax.bar(x - width / 2, p95_ms, width, label='P95', color='#42A5F5', edgecolor='white', linewidth=0.5)
    ax.bar(x + width / 2, p99_ms, width, label='P99', color='#EF5350', edgecolor='white', linewidth=0.5)

    ax.set_ylabel('Latency (ms)', fontsize=12)
    ax.set_xlabel('Algorithm / Parallelism', fontsize=12)
    ax.set_title('End-to-End Latency Percentiles (P95/P99)', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right', fontsize=10)
    ax.legend(loc='upper left', fontsize=10)
    ax.yaxis.grid(True, linestyle='--', alpha=0.3)
    ax.set_axisbelow(True)

    plt.tight_layout()

    full_path = f"{output_path}_latency_percentiles.{format}"
    plt.savefig(full_path, format=format, bbox_inches='tight', facecolor='white')
    plt.close()

    print(f"Latency percentile chart saved to: {full_path}")
    return True


# ============================================================================
# 综合仪表板
# ============================================================================

def generate_dashboard(
    results: Dict[str, Any], 
    output_path: str,
    format: str = 'png'
) -> bool:
    """生成综合仪表板（4个子图）
    
    Args:
        results: 测试结果字典
        output_path: 输出文件路径（不含扩展名）
        format: 输出格式
        
    Returns:
        是否成功生成图表
    """
    if not MATPLOTLIB_AVAILABLE:
        return False
    
    detailed = extract_detailed_results(results)
    if not detailed:
        print("No detailed results found for dashboard")
        return False
    
    # 创建 2x2 子图
    fig, axes = plt.subplots(2, 2, figsize=(16, 14))
    
    # 1. Breakdown 图（简化版）
    ax1 = axes[0, 0]
    _plot_breakdown_subplot(detailed, ax1)
    
    # 2. 吞吐量图
    ax2 = axes[0, 1]
    _plot_throughput_subplot(detailed, ax2)
    
    # 3. 召回率图
    ax3 = axes[1, 0]
    _plot_recall_subplot(detailed, ax3)
    
    # 4. 扩展性图
    ax4 = axes[1, 1]
    _plot_scalability_subplot(detailed, ax4)
    
    # 总标题
    fig.suptitle('SageFlow Join Performance Dashboard', 
                fontsize=16, fontweight='bold', y=0.98)
    
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    
    # 保存
    full_path = f"{output_path}_dashboard.{format}"
    plt.savefig(full_path, format=format, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"Dashboard saved to: {full_path}")
    return True


def _plot_breakdown_subplot(detailed: List[Dict], ax) -> None:
    """绘制 Breakdown 子图"""
    # 按算法分组，只取并行度=1的数据简化显示
    algo_breakdown = {}
    for result in detailed:
        algo = result.get('algorithm', 'unknown')
        para = result.get('parallelism', 1)
        if para == 1 and algo not in algo_breakdown:
            algo_breakdown[algo] = result.get('breakdown', {})
    
    if not algo_breakdown:
        # 如果没有 p=1 的数据，取任意一个
        for result in detailed:
            algo = result.get('algorithm', 'unknown')
            if algo not in algo_breakdown:
                algo_breakdown[algo] = result.get('breakdown', {})
    
    algorithms = sorted(algo_breakdown.keys())
    x = np.arange(len(algorithms))
    width = 0.6
    
    breakdown_data = {key: [] for key in BREAKDOWN_COLORS.keys()}
    for algo in algorithms:
        bd = algo_breakdown[algo]
        for key in BREAKDOWN_COLORS.keys():
            value_ns = bd.get(f'{key}_ns', 0)
            breakdown_data[key].append(value_ns / 1_000_000.0)
    
    bottom = np.zeros(len(algorithms))
    for key in BREAKDOWN_COLORS.keys():
        values = np.array(breakdown_data[key])
        if np.sum(values) > 0:
            ax.bar(x, values, width, label=BREAKDOWN_LABELS[key],
                  bottom=bottom, color=BREAKDOWN_COLORS[key])
            bottom += values
    
    ax.set_ylabel('Time (ms)')
    ax.set_title('Breakdown (p=1)')
    ax.set_xticks(x)
    ax.set_xticklabels(algorithms, rotation=45, ha='right')
    ax.legend(loc='upper right', fontsize=8)


def _plot_throughput_subplot(detailed: List[Dict], ax) -> None:
    """绘制吞吐量子图"""
    algo_data = defaultdict(lambda: defaultdict(list))
    for result in detailed:
        algo = result.get('algorithm', 'unknown')
        para = result.get('parallelism', 1)
        throughput = result.get('throughput_records_per_sec', 0)
        if throughput > 0:
            algo_data[algo][para].append(throughput)
    
    for algo in sorted(algo_data.keys()):
        para_data = algo_data[algo]
        parallelisms = sorted(para_data.keys())
        throughputs = [np.mean(para_data[p]) for p in parallelisms]
        
        color = ALGORITHM_COLORS.get(algo, '#333333')
        marker = ALGORITHM_MARKERS.get(algo, 'o')
        ax.plot(parallelisms, throughputs, label=algo, color=color, 
               marker=marker, linewidth=2, markersize=6)
    
    ax.set_xlabel('Parallelism')
    ax.set_ylabel('Throughput (rec/s)')
    ax.set_title('Throughput vs Parallelism')
    ax.legend(loc='best', fontsize=8)
    ax.grid(True, alpha=0.3)


def _plot_recall_subplot(detailed: List[Dict], ax) -> None:
    """绘制召回率子图"""
    algo_recall = defaultdict(list)
    for result in detailed:
        algo = result.get('algorithm', 'unknown')
        recall = result.get('recall', 0)
        algo_recall[algo].append(recall)
    
    algorithms = sorted(algo_recall.keys())
    avg_recalls = [np.mean(algo_recall[algo]) for algo in algorithms]
    
    colors = [ALGORITHM_COLORS.get(algo, '#333333') for algo in algorithms]
    bars = ax.bar(algorithms, avg_recalls, color=colors, edgecolor='white')
    
    ax.axhline(y=1.0, color='r', linestyle='--', alpha=0.5)
    ax.set_ylabel('Recall')
    ax.set_title('Average Recall by Algorithm')
    ax.set_ylim(0, 1.1)
    
    # 在柱子上方显示数值
    for bar, val in zip(bars, avg_recalls):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.02,
               f'{val:.3f}', ha='center', va='bottom', fontsize=9)


def _plot_scalability_subplot(detailed: List[Dict], ax) -> None:
    """绘制扩展性子图"""
    algo_data = defaultdict(lambda: defaultdict(list))
    for result in detailed:
        algo = result.get('algorithm', 'unknown')
        para = result.get('parallelism', 1)
        throughput = result.get('throughput_records_per_sec', 0)
        if throughput > 0:
            algo_data[algo][para].append(throughput)
    
    all_parallelisms = set()
    for algo in algo_data:
        para_data = algo_data[algo]
        all_parallelisms.update(para_data.keys())
        
        base_para = min(para_data.keys())
        base_throughput = np.mean(para_data[base_para])
        if base_throughput <= 0:
            continue
        
        parallelisms = sorted(para_data.keys())
        speedups = [np.mean(para_data[p]) / base_throughput for p in parallelisms]
        
        color = ALGORITHM_COLORS.get(algo, '#333333')
        marker = ALGORITHM_MARKERS.get(algo, 'o')
        ax.plot(parallelisms, speedups, label=algo, color=color,
               marker=marker, linewidth=2, markersize=6)
    
    # 理想线性扩展
    if all_parallelisms:
        max_para = max(all_parallelisms)
        ax.plot([1, max_para], [1, max_para], 'k--', alpha=0.5, label='Ideal')
    
    ax.set_xlabel('Parallelism')
    ax.set_ylabel('Speedup')
    ax.set_title('Scalability (Speedup vs p=1)')
    ax.legend(loc='upper left', fontsize=8)
    ax.grid(True, alpha=0.3)


# ============================================================================
# 主函数
# ============================================================================

def generate_charts(
    input_dir: str, 
    output_dir: Optional[str] = None,
    format: str = 'png',
    charts: Optional[List[str]] = None
) -> None:
    """生成所有图表
    
    Args:
        input_dir: 输入目录（包含 JSON 报告）
        output_dir: 输出目录（默认与输入相同）
        format: 输出格式
        charts: 要生成的图表列表，None 表示全部
    """
    if not MATPLOTLIB_AVAILABLE:
        print("Error: matplotlib is not available. Install with: pip install matplotlib numpy")
        return
    
    if output_dir is None:
        output_dir = input_dir
    
    # 确保输出目录存在
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # 加载结果
    print(f"\nLoading results from: {input_dir}")
    results = load_results(input_dir)
    
    if not results:
        print(f"No result files found in {input_dir}")
        return
    
    print(f"Found {len(results)} report(s)")
    
    # 确定输出基础路径
    output_base = str(Path(output_dir) / 'chart')
    
    # 可用图表
    available_charts = {
        'breakdown': generate_breakdown_chart,
        'throughput': generate_throughput_chart,
        'recall': generate_recall_comparison_chart,
        'scalability': generate_scalability_chart,
        'latency': generate_latency_percentile_chart,
        'dashboard': generate_dashboard,
    }
    
    # 确定要生成的图表
    if charts is None or 'all' in charts:
        charts_to_generate = list(available_charts.keys())
    else:
        charts_to_generate = [c for c in charts if c in available_charts]
    
    # 生成图表
    print(f"\nGenerating charts (format: {format})...")
    
    success_count = 0
    for chart_name in charts_to_generate:
        func = available_charts[chart_name]
        try:
            if func(results, output_base, format):
                success_count += 1
        except Exception as e:
            print(f"Error generating {chart_name} chart: {e}")
    
    print(f"\nGenerated {success_count}/{len(charts_to_generate)} charts in: {output_dir}")


def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(
        description='Visualize SageFlow test results',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --input-dir test/result/integration
  %(prog)s --input-dir test/result/integration --format svg
  %(prog)s --input-dir test/result/integration --charts breakdown throughput
  %(prog)s --input-dir test/result/integration --charts all
        """
    )
    
    parser.add_argument(
        '--input-dir', '-i',
        type=str,
        default='test/result/integration',
        help='Directory containing result JSON files (default: test/result/integration)'
    )
    
    parser.add_argument(
        '--output-dir', '-o',
        type=str,
        default=None,
        help='Output directory for charts (default: same as input)'
    )
    
    parser.add_argument(
        '--format', '-f',
        type=str,
        default='png',
        choices=['png', 'svg', 'pdf'],
        help='Output format for charts (default: png)'
    )
    
    parser.add_argument(
        '--charts', '-c',
        nargs='+',
        choices=['breakdown', 'throughput', 'recall', 'scalability', 'latency', 'dashboard', 'all'],
        default=['all'],
        help='Charts to generate (default: all)'
    )
    
    args = parser.parse_args()
    
    generate_charts(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        format=args.format,
        charts=args.charts
    )


if __name__ == '__main__':
    main()
