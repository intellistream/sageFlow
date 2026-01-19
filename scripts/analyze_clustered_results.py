#!/usr/bin/env python3
"""
ClusteredJoin 实验结果分析和可视化脚本

从 CSV 文件中提取实验结果并生成分析报告和图表。

使用方法：
  python scripts/analyze_clustered_results.py -i test/result/clustered_experiment -o test/result/charts

Author: SageFlow Team
Date: 2024-01-05
"""

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

# 尝试导入 matplotlib
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("Warning: matplotlib not installed. Text-only analysis will be provided.")

# ============================================================================
# CSV 数据加载
# ============================================================================

def load_csv_results(result_dir: str) -> List[Dict]:
    """加载所有 CSV 结果文件"""
    results = []
    result_path = Path(result_dir)
    
    if not result_path.exists():
        print(f"Error: Result directory does not exist: {result_dir}")
        return results
    
    csv_files = list(result_path.glob('*.csv'))
    print(f"Found {len(csv_files)} CSV files")
    
    for csv_file in csv_files:
        try:
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # 转换数值类型
                    row['recall'] = float(row.get('recall', 0))
                    row['precision'] = float(row.get('precision', 0))
                    row['execution_time_ms'] = float(row.get('execution_time_ms', 0))
                    row['parallelism'] = int(row.get('parallelism', 0))
                    row['data_size'] = int(row.get('data_size', 0))
                    row['expected_count'] = int(row.get('expected_count', 0))
                    row['actual_count'] = int(row.get('actual_count', 0))
                    row['source_file'] = csv_file.stem
                    results.append(row)
        except Exception as e:
            print(f"Warning: Error reading {csv_file}: {e}")
    
    return results


# ============================================================================
# 数据分析
# ============================================================================

def analyze_exp_a(results: List[Dict]) -> Tuple[Dict, List[int], List[float]]:
    """分析实验 A (overlap_ratio × parallelism)"""
    parallelisms = [1, 2, 4, 8, 16]
    ratios = [0.01, 0.02, 0.05, 0.10, 0.20]
    
    exp_a_data = {}
    
    for row in results:
        test_name = row.get('test_name', '')
        if not test_name.startswith('exp_a_'):
            continue
        
        try:
            # 解析 exp_a_p{p}_r{ratio}
            parts = test_name.split('_')
            p = int(parts[2][1:])  # p8 -> 8
            r_str = parts[3][1:]   # r010 -> 010 or r001 -> 001
            
            # 转换 ratio
            if len(r_str) == 3:
                r = int(r_str) / 100  # r010 -> 0.10, r001 -> 0.01
            else:
                r = float(r_str) / 100
            
            key = (p, r)
            exp_a_data[key] = {
                'recall': row['recall'],
                'time_ms': row['execution_time_ms'],
                'parallelism': p,
                'overlap_ratio': r
            }
        except Exception as e:
            print(f"Warning: Failed to parse '{test_name}': {e}")
    
    return exp_a_data, parallelisms, ratios


def analyze_exp_b(results: List[Dict]) -> Dict:
    """分析实验 B (multicast_k sweep)"""
    exp_b_data = {}
    
    for row in results:
        test_name = row.get('test_name', '')
        if not test_name.startswith('exp_b_k'):
            continue
        
        try:
            # 解析 exp_b_k{k}
            k = int(test_name.split('_')[2][1:])  # exp_b_k8 -> 8
            exp_b_data[k] = {
                'recall': row['recall'],
                'time_ms': row['execution_time_ms'],
                'parallelism': row['parallelism']
            }
        except Exception as e:
            print(f"Warning: Failed to parse '{test_name}': {e}")
    
    return exp_b_data


# ============================================================================
# 文本报告生成
# ============================================================================

def generate_text_report(exp_a_data: Dict, exp_b_data: Dict, 
                         parallelisms: List[int], ratios: List[float]) -> str:
    """生成文本分析报告"""
    lines = []
    lines.append("=" * 70)
    lines.append("ClusteredJoin 实验结果分析报告")
    lines.append("=" * 70)
    lines.append("")
    
    # 实验 A 报告
    lines.append("【实验 A】Overlap Ratio vs Parallelism (k=0 模式)")
    lines.append("-" * 70)
    lines.append("")
    
    # 表头
    p_r_label = "p \\ r"
    header = f"{p_r_label:<8}"
    for r in ratios:
        header += f"  {r:.2f}"
    lines.append(header)
    lines.append("-" * 60)
    
    # 数据行
    for p in parallelisms:
        row = f"p={p:<5}"
        for r in ratios:
            data = exp_a_data.get((p, r))
            if data:
                row += f" {data['recall']:.2f}"
            else:
                row += "  N/A"
        lines.append(row)
    
    lines.append("")
    lines.append("关键发现：")
    
    # 分析趋势
    for p in parallelisms:
        recalls = []
        for r in ratios:
            data = exp_a_data.get((p, r))
            if data:
                recalls.append((r, data['recall']))
        if recalls:
            max_recall = max(recalls, key=lambda x: x[1])
            min_recall = min(recalls, key=lambda x: x[1])
            lines.append(f"  - p={p}: recall 范围 [{min_recall[1]:.2f} @ r={min_recall[0]:.2f}] ~ [{max_recall[1]:.2f} @ r={max_recall[0]:.2f}]")
    
    lines.append("")
    lines.append("")
    
    # 实验 B 报告
    lines.append("【实验 B】Multicast K Sweep (p=32)")
    lines.append("-" * 70)
    lines.append("")
    
    if exp_b_data:
        lines.append(f"{'k':<8} {'Recall':<10} {'Time(ms)':<12}")
        lines.append("-" * 35)
        for k in sorted(exp_b_data.keys()):
            data = exp_b_data[k]
            lines.append(f"k={k:<5} {data['recall']:<10.4f} {data['time_ms']:<12.2f}")
        
        lines.append("")
        lines.append("关键发现：")
        k_values = sorted(exp_b_data.keys())
        if len(k_values) >= 2:
            min_k = k_values[0]
            max_k = k_values[-1]
            lines.append(f"  - k 从 {min_k} 增加到 {max_k}:")
            lines.append(f"    - Recall: {exp_b_data[min_k]['recall']:.4f} → {exp_b_data[max_k]['recall']:.4f}")
            lines.append(f"    - Time: {exp_b_data[min_k]['time_ms']:.2f}ms → {exp_b_data[max_k]['time_ms']:.2f}ms")
    else:
        lines.append("  无数据")
    
    lines.append("")
    lines.append("=" * 70)
    lines.append("")
    
    return "\n".join(lines)


# ============================================================================
# 可视化
# ============================================================================

def plot_exp_a_heatmap(exp_a_data: Dict, parallelisms: List[int], 
                       ratios: List[float], output_dir: str) -> None:
    """绘制实验 A 召回率热力图"""
    if not MATPLOTLIB_AVAILABLE:
        print("matplotlib not available, skipping heatmap")
        return
    
    # 构建矩阵
    recall_matrix = np.full((len(parallelisms), len(ratios)), np.nan)
    time_matrix = np.full((len(parallelisms), len(ratios)), np.nan)
    
    for i, p in enumerate(parallelisms):
        for j, r in enumerate(ratios):
            data = exp_a_data.get((p, r))
            if data:
                recall_matrix[i, j] = data['recall']
                time_matrix[i, j] = data['time_ms'] / 1000  # 转换为秒
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    # 召回率热力图
    ax1 = axes[0]
    im1 = ax1.imshow(recall_matrix, cmap='RdYlGn', aspect='auto', vmin=0, vmax=1)
    ax1.set_xticks(range(len(ratios)))
    ax1.set_xticklabels([f'{r:.2f}' for r in ratios])
    ax1.set_yticks(range(len(parallelisms)))
    ax1.set_yticklabels([f'p={p}' for p in parallelisms])
    ax1.set_xlabel('Overlap Ratio', fontsize=12)
    ax1.set_ylabel('Parallelism', fontsize=12)
    ax1.set_title('Recall Rate (k=0 mode)', fontsize=14)
    plt.colorbar(im1, ax=ax1, label='Recall')
    
    # 添加数值标注
    for i in range(len(parallelisms)):
        for j in range(len(ratios)):
            if not np.isnan(recall_matrix[i, j]):
                ax1.text(j, i, f'{recall_matrix[i, j]:.2f}',
                        ha='center', va='center', color='black', fontsize=10)
    
    # 耗时热力图
    ax2 = axes[1]
    im2 = ax2.imshow(time_matrix, cmap='YlOrRd', aspect='auto')
    ax2.set_xticks(range(len(ratios)))
    ax2.set_xticklabels([f'{r:.2f}' for r in ratios])
    ax2.set_yticks(range(len(parallelisms)))
    ax2.set_yticklabels([f'p={p}' for p in parallelisms])
    ax2.set_xlabel('Overlap Ratio', fontsize=12)
    ax2.set_ylabel('Parallelism', fontsize=12)
    ax2.set_title('Execution Time (seconds)', fontsize=14)
    plt.colorbar(im2, ax=ax2, label='Time (s)')
    
    # 添加数值标注
    for i in range(len(parallelisms)):
        for j in range(len(ratios)):
            if not np.isnan(time_matrix[i, j]):
                ax2.text(j, i, f'{time_matrix[i, j]:.1f}',
                        ha='center', va='center', color='black', fontsize=10)
    
    plt.tight_layout()
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path / 'exp_a_heatmaps.png', bbox_inches='tight')
    print(f"Saved: {output_path / 'exp_a_heatmaps.png'}")
    plt.close()


def plot_exp_a_lines(exp_a_data: Dict, parallelisms: List[int], 
                     ratios: List[float], output_dir: str) -> None:
    """绘制实验 A 折线图：固定 overlap_ratio，变化 parallelism"""
    if not MATPLOTLIB_AVAILABLE:
        return
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    colors = plt.cm.viridis(np.linspace(0, 1, len(ratios)))
    
    for idx, r in enumerate(ratios):
        recalls = []
        ps = []
        for p in parallelisms:
            data = exp_a_data.get((p, r))
            if data:
                ps.append(p)
                recalls.append(data['recall'])
        
        if recalls:
            ax.plot(ps, recalls, 'o-', color=colors[idx], label=f'r={r:.2f}', linewidth=2, markersize=8)
    
    ax.set_xlabel('Parallelism', fontsize=12)
    ax.set_ylabel('Recall', fontsize=12)
    ax.set_title('Recall vs Parallelism (Fixed Overlap Ratio)', fontsize=14)
    ax.set_ylim(0, 1.1)
    ax.set_xscale('log', base=2)
    ax.set_xticks(parallelisms)
    ax.set_xticklabels([str(p) for p in parallelisms])
    ax.legend(loc='lower left')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path / 'exp_a_recall_vs_parallelism.png', bbox_inches='tight')
    print(f"Saved: {output_path / 'exp_a_recall_vs_parallelism.png'}")
    plt.close()


def plot_exp_b_dual_axis(exp_b_data: Dict, output_dir: str) -> None:
    """绘制实验 B 双轴图：Multicast K vs Recall/Time"""
    if not MATPLOTLIB_AVAILABLE or not exp_b_data:
        return
    
    k_values = sorted(exp_b_data.keys())
    recalls = [exp_b_data[k]['recall'] for k in k_values]
    times = [exp_b_data[k]['time_ms'] / 1000 for k in k_values]  # 转换为秒
    
    fig, ax1 = plt.subplots(figsize=(10, 6))
    
    # 召回率（左轴）
    color1 = '#2196F3'
    ax1.plot(k_values, recalls, 'o-', color=color1, linewidth=2, markersize=10, label='Recall')
    ax1.set_xlabel('Multicast K', fontsize=12)
    ax1.set_ylabel('Recall', fontsize=12, color=color1)
    ax1.tick_params(axis='y', labelcolor=color1)
    ax1.set_ylim(0, 1.1)
    
    # 耗时（右轴）
    ax2 = ax1.twinx()
    color2 = '#F44336'
    ax2.plot(k_values, times, 's--', color=color2, linewidth=2, markersize=10, label='Time')
    ax2.set_ylabel('Execution Time (seconds)', fontsize=12, color=color2)
    ax2.tick_params(axis='y', labelcolor=color2)
    
    # 合并图例
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='center right')
    
    plt.title('Multicast K Sweep (p=32)', fontsize=14)
    ax1.grid(True, alpha=0.3)
    
    plt.tight_layout()
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path / 'exp_b_multicast_k.png', bbox_inches='tight')
    print(f"Saved: {output_path / 'exp_b_multicast_k.png'}")
    plt.close()


# ============================================================================
# 主函数
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Analyze ClusteredJoin experiment results')
    parser.add_argument('-i', '--input', type=str, default='test/result/clustered_experiment',
                       help='Input directory with CSV result files')
    parser.add_argument('-o', '--output', type=str, default='test/result/charts',
                       help='Output directory for charts')
    parser.add_argument('--no-plot', action='store_true',
                       help='Skip chart generation (text report only)')
    
    args = parser.parse_args()
    
    # 加载数据
    print(f"\nLoading results from: {args.input}")
    results = load_csv_results(args.input)
    
    if not results:
        print("No results found. Exiting.")
        sys.exit(1)
    
    print(f"Loaded {len(results)} result records")
    
    # 分析数据
    exp_a_data, parallelisms, ratios = analyze_exp_a(results)
    exp_b_data = analyze_exp_b(results)
    
    print(f"\nExperiment A: {len(exp_a_data)} data points")
    print(f"Experiment B: {len(exp_b_data)} data points")
    
    # 生成文本报告
    report = generate_text_report(exp_a_data, exp_b_data, parallelisms, ratios)
    print("\n" + report)
    
    # 保存报告
    output_path = Path(args.output)
    output_path.mkdir(parents=True, exist_ok=True)
    report_file = output_path / 'experiment_report.txt'
    with open(report_file, 'w') as f:
        f.write(report)
    print(f"Report saved to: {report_file}")
    
    # 生成图表
    if not args.no_plot and MATPLOTLIB_AVAILABLE:
        print("\nGenerating charts...")
        plot_exp_a_heatmap(exp_a_data, parallelisms, ratios, args.output)
        plot_exp_a_lines(exp_a_data, parallelisms, ratios, args.output)
        plot_exp_b_dual_axis(exp_b_data, args.output)
        print("Done!")
    elif not MATPLOTLIB_AVAILABLE:
        print("\nSkipping charts (matplotlib not installed)")


if __name__ == '__main__':
    main()
