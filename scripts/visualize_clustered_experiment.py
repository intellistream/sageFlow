#!/usr/bin/env python3
"""
ClusteredJoin 实验结果可视化脚本

生成以下图表：
1. 实验 A：Overlap Ratio × Parallelism 热力图（召回率和耗时）
2. 实验 A：固定 p 的多条折线图
3. 实验 B：Multicast K 双轴折线图

使用方法：
  python scripts/visualize_clustered_experiment.py -i test/result/integration -o test/result/charts

Author: SageFlow Team
Date: 2026-01-05
"""

import json
import argparse
import sys
import csv
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
from collections import defaultdict

# 延迟导入 matplotlib
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
# 配置
# ============================================================================

if MATPLOTLIB_AVAILABLE:
    plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial', 'Helvetica']
    plt.rcParams['axes.unicode_minus'] = False
    plt.rcParams['figure.dpi'] = 100
    plt.rcParams['savefig.dpi'] = 150
    plt.rcParams['figure.figsize'] = (12, 8)

# 颜色方案
COLORS = {
    'recall': '#2196F3',      # 蓝色
    'time': '#F44336',        # 红色
    'throughput': '#4CAF50',  # 绿色
}

# ============================================================================
# 数据加载
# ============================================================================

def load_experiment_results(result_dir: str) -> Dict[str, Any]:
    """加载实验结果 JSON 文件"""
    results = {}
    result_path = Path(result_dir)
    
    if not result_path.exists():
        print(f"Warning: Result directory does not exist: {result_dir}")
        return results
    
    # 查找所有 JSON 文件
    json_files = list(result_path.glob('*.json'))
    
    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
                key = json_file.stem
                results[key] = data
                print(f"Loaded: {json_file.name}")
        except json.JSONDecodeError as e:
            print(f"Warning: Failed to parse {json_file}: {e}")
        except Exception as e:
            print(f"Warning: Error reading {json_file}: {e}")
    
    return results


def load_experiment_results_from_csv(result_dir: str) -> Dict[str, Any]:
    """
    加载实验结果 CSV 文件（*_results.csv）。
    
    说明：集成测试会为每个 test_case 输出一个 CSV（通常只有 1 行），字段包含：
      - test_name
      - recall
      - execution_time_ms
    我们将其转换成与 report.json 兼容的结构：{"csv": {"detailed_results": [...]}}，
    以复用后续 extract_* 逻辑。
    """
    result_path = Path(result_dir)
    detailed: List[Dict[str, Any]] = []
    
    for csv_file in sorted(result_path.glob("*_results.csv")):
        try:
            with open(csv_file, "r", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    name = row.get("test_name") or row.get("test_case_name") or csv_file.stem.replace("_results", "")
                    recall = float(row.get("recall", "") or 0.0)
                    # 时间字段：分别保留算法口径与端到端口径，避免把 0.00 当作有效值误用
                    execution_time_ms = float(row.get("execution_time_ms", "") or row.get("total_time_ms", "") or 0.0)
                    join_time_ms = float(row.get("join_time_ms", "") or 0.0)
                    sink_wait_ms = float(row.get("sink_wait_ms", "") or 0.0)

                    total_emits = int(float(row.get("total_emits", "") or 0.0))
                    sink_processed = int(float(row.get("sink_processed", "") or 0.0))
                    sink_dedup = int(float(row.get("sink_dedup", "") or 0.0))
                    detailed.append({
                        "test_case_name": name,
                        "recall": recall,
                        "execution_time_ms": execution_time_ms,
                        "join_time_ms": join_time_ms,
                        "sink_wait_ms": sink_wait_ms,
                        "total_emits": total_emits,
                        "sink_processed": sink_processed,
                        "sink_dedup": sink_dedup,
                        "source_report": csv_file.name,
                    })
        except Exception as e:
            print(f"Warning: Failed to read {csv_file}: {e}")
    
    if not detailed:
        return {}
    
    return {"csv": {"detailed_results": detailed}}


def load_experiment_results_auto(result_dir: str) -> Dict[str, Any]:
    """自动加载结果：优先 *_results.csv，其次 *.json（保持向后兼容）"""
    p = Path(result_dir)
    if not p.exists():
        print(f"Warning: Result directory does not exist: {result_dir}")
        return {}
    if any(p.glob("*_results.csv")):
        return load_experiment_results_from_csv(result_dir)
    return load_experiment_results(result_dir)


def extract_detailed_results(results: Dict[str, Any]) -> List[Dict]:
    """从报告中提取详细结果"""
    detailed = []
    
    for report_name, data in results.items():
        if isinstance(data, dict):
            if 'detailed_results' in data:
                for result in data['detailed_results']:
                    result['source_report'] = report_name
                    detailed.append(result)
            elif 'test_case_name' in data:
                # 单个结果文件
                data['source_report'] = report_name
                detailed.append(data)
    
    return detailed


# ============================================================================
# 实验 A 数据提取
# ============================================================================

def extract_exp_a_data(results: Dict[str, Any]) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, List[int], List[float]]:
    """提取实验 A 数据，返回召回率、join_time_ms、sink_wait_ms、total_emits 矩阵"""
    parallelisms = [1, 2, 4, 8, 16]
    ratios = [0.01, 0.02, 0.05, 0.10, 0.20]
    
    recall_matrix = np.full((len(parallelisms), len(ratios)), np.nan)
    join_time_matrix = np.full((len(parallelisms), len(ratios)), np.nan)
    sink_wait_matrix = np.full((len(parallelisms), len(ratios)), np.nan)
    emits_matrix = np.full((len(parallelisms), len(ratios)), np.nan)
    
    detailed = extract_detailed_results(results)
    
    for result in detailed:
        name = result.get('test_case_name', result.get('name', ''))
        if not name.startswith('exp_a_'):
            continue
        
        try:
            # 解析 p 和 r
            # 格式: exp_a_p{p}_r{r*100}  例如 exp_a_p8_r010
            parts = name.split('_')
            p = int(parts[2][1:])  # p8 -> 8
            r_str = parts[3][1:]   # r010 -> 010
            
            # 处理不同的 ratio 格式
            if len(r_str) == 3:
                r = int(r_str) / 100  # r010 -> 0.10
            elif len(r_str) == 2:
                r = int(r_str) / 100  # r01 -> 0.01
            else:
                r = float(r_str) / 10  # r1 -> 0.1
            
            if p in parallelisms and r in ratios:
                i = parallelisms.index(p)
                j = ratios.index(r)
                recall_matrix[i, j] = result.get('recall', 0)
                # 默认用算法口径 join_time_ms；如果缺失/为 0，则回退到端到端 execution_time_ms
                jt = float(result.get('join_time_ms', 0) or 0.0)
                et = float(result.get('total_time_ms', result.get('execution_time_ms', 0)) or 0.0)
                sw = float(result.get('sink_wait_ms', 0) or 0.0)
                te = float(result.get('total_emits', 0) or 0.0)
                join_time_matrix[i, j] = jt if jt > 0 else et
                sink_wait_matrix[i, j] = sw if sw > 0 else 0.0
                emits_matrix[i, j] = te if te > 0 else np.nan
                
        except (IndexError, ValueError) as e:
            print(f"Warning: Failed to parse test case name '{name}': {e}")
            continue
    
    return recall_matrix, join_time_matrix, sink_wait_matrix, emits_matrix, parallelisms, ratios


# ============================================================================
# 实验 B 数据提取
# ============================================================================

def extract_exp_b_data(results: Dict[str, Any]) -> Tuple[List[int], List[float], List[float]]:
    """提取实验 B 数据，返回 k 值、召回率、耗时列表"""
    k_values = []
    recalls = []
    times = []
    
    detailed = extract_detailed_results(results)
    
    for result in detailed:
        name = result.get('test_case_name', result.get('name', ''))
        if not name.startswith('exp_b_k'):
            continue
        
        try:
            # 解析 k: exp_b_k{k}
            k = int(name.split('_')[2][1:])  # exp_b_k8 -> 8
            k_values.append(k)
            recalls.append(result.get('recall', 0))
            times.append(result.get('total_time_ms', 
                                    result.get('execution_time_ms', 0)))
        except (IndexError, ValueError) as e:
            print(f"Warning: Failed to parse test case name '{name}': {e}")
            continue
    
    # 按 k 排序
    if k_values:
        sorted_data = sorted(zip(k_values, recalls, times))
        k_values = [x[0] for x in sorted_data]
        recalls = [x[1] for x in sorted_data]
        times = [x[2] for x in sorted_data]
    
    return k_values, recalls, times


# ============================================================================
# 可视化函数
# ============================================================================

def plot_exp_a_heatmaps(recall_matrix: np.ndarray, time_matrix: np.ndarray, 
                        parallelisms: List[int], ratios: List[float], 
                        output_dir: str) -> None:
    """绘制实验 A 热力图"""
    if not MATPLOTLIB_AVAILABLE:
        print("matplotlib not available, skipping heatmaps")
        return
    
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
                text = ax1.text(j, i, f'{recall_matrix[i, j]:.2f}',
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
    ax2.set_title('Algorithm Time (Join emits stable, k=0 mode)', fontsize=14)
    plt.colorbar(im2, ax=ax2, label='Time (ms)')
    
    # 添加数值标注
    for i in range(len(parallelisms)):
        for j in range(len(ratios)):
            if not np.isnan(time_matrix[i, j]):
                text = ax2.text(j, i, f'{time_matrix[i, j]:.0f}',
                              ha='center', va='center', color='black', fontsize=9)
    
    plt.tight_layout()
    output_path = f'{output_dir}/exp_a_heatmaps.png'
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")


def plot_exp_a_sink_wait_heatmap(sink_wait_matrix: np.ndarray,
                                 parallelisms: List[int], ratios: List[float],
                                 output_dir: str) -> None:
    """绘制实验 A 的 Sink 追赶等待热力图（用于解释“台阶”）"""
    if not MATPLOTLIB_AVAILABLE:
        print("matplotlib not available, skipping sink-wait heatmap")
        return
    
    fig, ax = plt.subplots(1, 1, figsize=(8, 6))
    im = ax.imshow(sink_wait_matrix, cmap='PuBu', aspect='auto')
    ax.set_xticks(range(len(ratios)))
    ax.set_xticklabels([f'{r:.2f}' for r in ratios])
    ax.set_yticks(range(len(parallelisms)))
    ax.set_yticklabels([f'p={p}' for p in parallelisms])
    ax.set_xlabel('Overlap Ratio', fontsize=12)
    ax.set_ylabel('Parallelism', fontsize=12)
    ax.set_title('Sink Catch-up Wait Time (ms)', fontsize=14)
    plt.colorbar(im, ax=ax, label='Time (ms)')
    
    for i in range(len(parallelisms)):
        for j in range(len(ratios)):
            if not np.isnan(sink_wait_matrix[i, j]):
                ax.text(j, i, f'{sink_wait_matrix[i, j]:.0f}',
                        ha='center', va='center', color='black', fontsize=9)
    
    plt.tight_layout()
    output_path = f'{output_dir}/exp_a_sink_wait_heatmap.png'
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")


def plot_exp_a_total_emits_heatmap(emits_matrix: np.ndarray,
                                   parallelisms: List[int], ratios: List[float],
                                   output_dir: str) -> None:
    """绘制实验 A 的 total_emits 热力图（用于直接观察 workload 变化）"""
    if not MATPLOTLIB_AVAILABLE:
        print("matplotlib not available, skipping total-emits heatmap")
        return
    
    fig, ax = plt.subplots(1, 1, figsize=(8, 6))
    im = ax.imshow(emits_matrix, cmap='Greys', aspect='auto')
    ax.set_xticks(range(len(ratios)))
    ax.set_xticklabels([f'{r:.2f}' for r in ratios])
    ax.set_yticks(range(len(parallelisms)))
    ax.set_yticklabels([f'p={p}' for p in parallelisms])
    ax.set_xlabel('Overlap Ratio', fontsize=12)
    ax.set_ylabel('Parallelism', fontsize=12)
    ax.set_title('Total Emits (Join output, incl duplicates)', fontsize=14)
    plt.colorbar(im, ax=ax, label='Emits')
    
    for i in range(len(parallelisms)):
        for j in range(len(ratios)):
            if not np.isnan(emits_matrix[i, j]):
                ax.text(j, i, f'{emits_matrix[i, j]/1e6:.2f}M',
                        ha='center', va='center', color='black', fontsize=9)
    
    plt.tight_layout()
    output_path = f'{output_dir}/exp_a_total_emits_heatmap.png'
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")


def plot_exp_a_lines(recall_matrix: np.ndarray, time_matrix: np.ndarray,
                     parallelisms: List[int], ratios: List[float],
                     output_dir: str) -> None:
    """绘制实验 A 折线图"""
    if not MATPLOTLIB_AVAILABLE:
        print("matplotlib not available, skipping line plots")
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    colors = plt.cm.viridis(np.linspace(0, 1, len(parallelisms)))
    ratio_colors = plt.cm.plasma(np.linspace(0, 1, len(ratios)))
    
    # 图1：固定 p，不同 r 的召回率曲线
    ax1 = axes[0, 0]
    for i, p in enumerate(parallelisms):
        valid_mask = ~np.isnan(recall_matrix[i, :])
        if valid_mask.any():
            valid_ratios = np.array(ratios)[valid_mask]
            valid_recalls = recall_matrix[i, :][valid_mask]
            ax1.plot(valid_ratios, valid_recalls, 'o-', color=colors[i], 
                    label=f'p={p}', linewidth=2, markersize=8)
    ax1.set_xlabel('Overlap Ratio', fontsize=11)
    ax1.set_ylabel('Recall', fontsize=11)
    ax1.set_title('Recall vs Overlap Ratio (Fixed Parallelism)', fontsize=12)
    ax1.legend(loc='lower right')
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(0, 1.05)
    
    # 图2：固定 r，不同 p 的召回率曲线
    ax2 = axes[0, 1]
    for j, r in enumerate(ratios):
        valid_mask = ~np.isnan(recall_matrix[:, j])
        if valid_mask.any():
            valid_ps = np.array(parallelisms)[valid_mask]
            valid_recalls = recall_matrix[:, j][valid_mask]
            ax2.plot(valid_ps, valid_recalls, 's-', color=ratio_colors[j],
                    label=f'r={r:.2f}', linewidth=2, markersize=8)
    ax2.set_xlabel('Parallelism', fontsize=11)
    ax2.set_ylabel('Recall', fontsize=11)
    ax2.set_title('Recall vs Parallelism (Fixed Overlap Ratio)', fontsize=12)
    ax2.legend(loc='lower right')
    ax2.grid(True, alpha=0.3)
    ax2.set_ylim(0, 1.05)
    ax2.set_xscale('log', base=2)
    ax2.set_xticks(parallelisms)
    ax2.set_xticklabels([str(p) for p in parallelisms])
    
    # 图3：固定 p，不同 r 的耗时曲线
    ax3 = axes[1, 0]
    for i, p in enumerate(parallelisms):
        valid_mask = ~np.isnan(time_matrix[i, :])
        if valid_mask.any():
            valid_ratios = np.array(ratios)[valid_mask]
            valid_times = time_matrix[i, :][valid_mask]
            ax3.plot(valid_ratios, valid_times, 'o-', color=colors[i], 
                    label=f'p={p}', linewidth=2, markersize=8)
    ax3.set_xlabel('Overlap Ratio', fontsize=11)
    ax3.set_ylabel('Time (ms)', fontsize=11)
    ax3.set_title('Time vs Overlap Ratio (Fixed Parallelism)', fontsize=12)
    ax3.legend(loc='upper right')
    ax3.grid(True, alpha=0.3)
    
    # 图4：固定 r，不同 p 的耗时曲线
    ax4 = axes[1, 1]
    for j, r in enumerate(ratios):
        valid_mask = ~np.isnan(time_matrix[:, j])
        if valid_mask.any():
            valid_ps = np.array(parallelisms)[valid_mask]
            valid_times = time_matrix[:, j][valid_mask]
            ax4.plot(valid_ps, valid_times, 's-', color=ratio_colors[j],
                    label=f'r={r:.2f}', linewidth=2, markersize=8)
    ax4.set_xlabel('Parallelism', fontsize=11)
    ax4.set_ylabel('Time (ms)', fontsize=11)
    ax4.set_title('Time vs Parallelism (Fixed Overlap Ratio)', fontsize=12)
    ax4.legend(loc='upper right')
    ax4.grid(True, alpha=0.3)
    ax4.set_xscale('log', base=2)
    ax4.set_xticks(parallelisms)
    ax4.set_xticklabels([str(p) for p in parallelisms])
    
    plt.tight_layout()
    output_path = f'{output_dir}/exp_a_lines.png'
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")


def plot_exp_b(k_values: List[int], recalls: List[float], times: List[float],
               output_dir: str, num_partitions: int = 32) -> None:
    """绘制实验 B 双轴折线图"""
    if not MATPLOTLIB_AVAILABLE:
        print("matplotlib not available, skipping exp B plots")
        return
    
    if not k_values:
        print("No Experiment B data to plot")
        return
    
    # 图1：双轴折线图
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    color1 = COLORS['recall']
    color2 = COLORS['time']
    
    # 召回率（主 Y 轴）
    ax1.set_xlabel('Multicast K', fontsize=12)
    ax1.set_ylabel('Recall', color=color1, fontsize=12)
    line1 = ax1.plot(k_values, recalls, 'o-', color=color1, linewidth=2.5, 
                     markersize=10, label='Recall')
    ax1.tick_params(axis='y', labelcolor=color1)
    ax1.set_ylim(0, 1.05)
    ax1.grid(True, alpha=0.3)
    
    # 耗时（副 Y 轴）
    ax2 = ax1.twinx()
    ax2.set_ylabel('Time (ms)', color=color2, fontsize=12)
    line2 = ax2.plot(k_values, times, 's--', color=color2, linewidth=2.5, 
                     markersize=10, label='Time')
    ax2.tick_params(axis='y', labelcolor=color2)
    
    # 合并图例
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc='center right', fontsize=11)
    
    # 添加覆盖率标注（副 X 轴）
    ax3 = ax1.twiny()
    ax3.set_xlim(ax1.get_xlim())
    coverage_ticks = [k / num_partitions * 100 for k in k_values]
    ax3.set_xticks(k_values)
    ax3.set_xticklabels([f'{c:.1f}%' for c in coverage_ticks])
    ax3.set_xlabel(f'Coverage Rate (k/{num_partitions})', fontsize=10)
    
    plt.title(f'Multicast K vs Recall/Time (p={num_partitions})', fontsize=14, pad=20)
    plt.tight_layout()
    output_path = f'{output_dir}/exp_b_dual_axis.png'
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")
    
    # 图2：条形图+折线图组合
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    x = np.arange(len(k_values))
    width = 0.6
    
    bars = ax1.bar(x, recalls, width, color=color1, alpha=0.7, label='Recall')
    ax1.set_xlabel('Multicast K', fontsize=12)
    ax1.set_ylabel('Recall', color=color1, fontsize=12)
    ax1.set_xticks(x)
    ax1.set_xticklabels([f'k={k}' for k in k_values])
    ax1.set_ylim(0, 1.1)
    ax1.tick_params(axis='y', labelcolor=color1)
    
    ax2 = ax1.twinx()
    line = ax2.plot(x, times, 's-', color=color2, linewidth=2.5, 
                    markersize=10, label='Time')
    ax2.set_ylabel('Time (ms)', color=color2, fontsize=12)
    ax2.tick_params(axis='y', labelcolor=color2)
    
    # 在条形图上标注数值
    for bar, recall in zip(bars, recalls):
        height = bar.get_height()
        ax1.annotate(f'{recall:.2f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=9)
    
    plt.title(f'Multicast K: Recall vs Time Trade-off (p={num_partitions})', fontsize=14)
    plt.tight_layout()
    output_path = f'{output_dir}/exp_b_bar_line.png'
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")
    
    # 图3：归一化对比图
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # 归一化到 [0, 1]
    recalls_norm = np.array(recalls)
    times_norm = (np.array(times) - min(times)) / (max(times) - min(times)) if max(times) > min(times) else np.zeros_like(times)
    
    ax.plot(k_values, recalls_norm, 'o-', color=color1, linewidth=2.5, 
            markersize=10, label='Recall (normalized)')
    ax.plot(k_values, times_norm, 's--', color=color2, linewidth=2.5, 
            markersize=10, label='Time (normalized)')
    
    ax.set_xlabel('Multicast K', fontsize=12)
    ax.set_ylabel('Normalized Value', fontsize=12)
    ax.set_title(f'Recall vs Time Trade-off (p={num_partitions})', fontsize=14)
    ax.legend(loc='center right', fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.set_ylim(-0.05, 1.1)
    
    # 找到帕累托最优点
    # 简单定义：recall - time_norm 最大的点
    scores = recalls_norm - times_norm
    best_idx = np.argmax(scores)
    ax.axvline(x=k_values[best_idx], color='green', linestyle=':', alpha=0.7,
               label=f'Suggested k={k_values[best_idx]}')
    ax.legend(loc='center right', fontsize=11)
    
    plt.tight_layout()
    output_path = f'{output_dir}/exp_b_normalized.png'
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")


def print_summary(recall_matrix: np.ndarray, time_matrix: np.ndarray,
                  parallelisms: List[int], ratios: List[float],
                  k_values: List[int], recalls_b: List[float], 
                  times_b: List[float]) -> None:
    """打印结果摘要"""
    print("\n" + "=" * 60)
    print("EXPERIMENT RESULTS SUMMARY")
    print("=" * 60)
    
    # 实验 A 摘要
    if not np.all(np.isnan(recall_matrix)):
        print("\n--- Experiment A: Overlap Ratio × Parallelism ---")
        print("\nRecall Matrix:")
        print("         ", end="")
        for r in ratios:
            print(f"r={r:.2f}  ", end="")
        print()
        for i, p in enumerate(parallelisms):
            print(f"p={p:2d}:    ", end="")
            for j in range(len(ratios)):
                if np.isnan(recall_matrix[i, j]):
                    print("  N/A   ", end="")
                else:
                    print(f" {recall_matrix[i, j]:.3f}  ", end="")
            print()
        
        print("\nKey Observations:")
        # 找最佳参数组合
        if not np.all(np.isnan(recall_matrix)):
            best_idx = np.unravel_index(np.nanargmax(recall_matrix), recall_matrix.shape)
            print(f"  - Best recall: {recall_matrix[best_idx]:.3f} at p={parallelisms[best_idx[0]]}, r={ratios[best_idx[1]]:.2f}")
    
    # 实验 B 摘要
    if k_values:
        print("\n--- Experiment B: Multicast K Scan (p=32) ---")
        print("\nk\tRecall\tTime(ms)\tCoverage")
        print("-" * 40)
        for k, recall, time in zip(k_values, recalls_b, times_b):
            coverage = k / 32 * 100
            print(f"{k}\t{recall:.3f}\t{time:.0f}\t\t{coverage:.1f}%")
        
        print("\nKey Observations:")
        # 找到 90% 召回率的最小 k
        for k, recall in zip(k_values, recalls_b):
            if recall >= 0.90:
                print(f"  - First k with ≥90% recall: k={k}")
                break
        
        # 找最佳权衡点
        if len(recalls_b) > 1:
            recalls_norm = np.array(recalls_b)
            times_norm = (np.array(times_b) - min(times_b)) / (max(times_b) - min(times_b)) if max(times_b) > min(times_b) else np.zeros(len(times_b))
            scores = recalls_norm - 0.5 * times_norm  # 召回率权重更高
            best_idx = np.argmax(scores)
            print(f"  - Suggested trade-off point: k={k_values[best_idx]} (recall={recalls_b[best_idx]:.3f})")
    
    print("\n" + "=" * 60)


def _format_markdown_table(headers: List[str], rows: List[List[str]]) -> str:
    """Format a GitHub-flavored markdown table."""
    lines: List[str] = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join(["---"] * len(headers)) + "|")
    for r in rows:
        lines.append("| " + " | ".join(r) + " |")
    return "\n".join(lines)


def write_markdown_summary(
    output_path: Path,
    recall_matrix: np.ndarray,
    time_matrix: np.ndarray,
    sink_wait_matrix: np.ndarray,
    parallelisms: List[int],
    ratios: List[float],
    k_values: List[int],
    recalls_b: List[float],
    times_b: List[float],
    charts_dir: Optional[Path] = None,
    num_partitions: int = 32,
) -> None:
    """Write a markdown summary alongside plots for reproducibility."""
    lines: List[str] = []
    lines.append("# ClusteredJoin 实验结果总结报告（Rerun）")
    lines.append("")
    lines.append(f"- **结果目录**: `{output_path.parent}`")
    if charts_dir is not None:
        lines.append(f"- **图表目录**: `{charts_dir}`")
    lines.append("")

    # Experiment A
    if not np.all(np.isnan(recall_matrix)):
        lines.append("## 实验 A 结果：Overlap Ratio × Parallelism（k=0）")
        lines.append("")
        # Recall matrix table
        headers = ["p \\ r"] + [f"{r:.2f}" for r in ratios]
        rows: List[List[str]] = []
        for i, p in enumerate(parallelisms):
            row = [f"p={p}"]
            for j in range(len(ratios)):
                v = recall_matrix[i, j]
                row.append("N/A" if np.isnan(v) else f"{v:.2f}")
            rows.append(row)
        lines.append("### 召回率矩阵")
        lines.append("")
        lines.append(_format_markdown_table(headers, rows))
        lines.append("")

        # Time matrix table (ms)
        rows_t: List[List[str]] = []
        for i, p in enumerate(parallelisms):
            row = [f"p={p}"]
            for j in range(len(ratios)):
                v = time_matrix[i, j]
                row.append("N/A" if np.isnan(v) else f"{v/1000.0:.1f}s")
            rows_t.append(row)
        lines.append("### 算法完成时间矩阵（Join emits stable，秒）")
        lines.append("")
        lines.append(_format_markdown_table(headers, rows_t))
        lines.append("")

        # Sink wait matrix table (s)
        rows_sw: List[List[str]] = []
        for i, p in enumerate(parallelisms):
            row = [f"p={p}"]
            for j in range(len(ratios)):
                v = sink_wait_matrix[i, j]
                row.append("N/A" if np.isnan(v) else f"{v/1000.0:.1f}s")
            rows_sw.append(row)
        lines.append("### Sink 追赶等待时间矩阵（秒）")
        lines.append("")
        lines.append(_format_markdown_table(headers, rows_sw))
        lines.append("")

    # Experiment B
    if k_values:
        lines.append("## 实验 B 结果：Multicast K Sweep（p=32）")
        lines.append("")
        headers_b = ["k", "Recall", "Time(ms)", "Coverage(k/p)"]
        rows_b: List[List[str]] = []
        for k, r, t in zip(k_values, recalls_b, times_b):
            cov = (k / float(num_partitions)) * 100.0
            rows_b.append([str(k), f"{r:.4f}", f"{t:.2f}", f"{cov:.1f}%"])
        lines.append(_format_markdown_table(headers_b, rows_b))
        lines.append("")

    # Charts
    if charts_dir is not None and charts_dir.exists():
        pngs = sorted([p.name for p in charts_dir.glob("*.png")])
        if pngs:
            lines.append("## 生成的图表")
            lines.append("")
            for name in pngs:
                lines.append(f"- `{name}`")
            lines.append("")

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote markdown summary: {output_path}")


# ============================================================================
# 主函数
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Visualize ClusteredJoin experiment results',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -i test/result/integration -o test/result/charts
  %(prog)s -i test/result/clustered_experiment
        """
    )
    parser.add_argument('--input-dir', '-i', type=str, required=True,
                       help='Directory containing experiment results (*.json or *_results.csv)')
    parser.add_argument('--output-dir', '-o', type=str, default=None,
                       help='Output directory for charts (default: same as input)')
    parser.add_argument('--num-partitions', '-p', type=int, default=32,
                       help='Number of partitions for Experiment B (default: 32)')
    args = parser.parse_args()
    
    output_dir = args.output_dir or args.input_dir
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    print(f"Loading results from: {args.input_dir}")
    results = load_experiment_results_auto(args.input_dir)
    
    if not results:
        print("No results found!")
        return 1
    
    print(f"Loaded {len(results)} result files")
    
    # 提取实验 A 数据
    recall_matrix, time_matrix, sink_wait_matrix, emits_matrix, parallelisms, ratios = extract_exp_a_data(results)
    exp_a_has_data = not np.all(np.isnan(recall_matrix))
    
    # 提取实验 B 数据
    k_values, recalls_b, times_b = extract_exp_b_data(results)
    exp_b_has_data = len(k_values) > 0
    
    # 生成图表
    if exp_a_has_data:
        print("\nGenerating Experiment A charts...")
        plot_exp_a_heatmaps(recall_matrix, time_matrix, parallelisms, ratios, output_dir)
        plot_exp_a_sink_wait_heatmap(sink_wait_matrix, parallelisms, ratios, output_dir)
        plot_exp_a_total_emits_heatmap(emits_matrix, parallelisms, ratios, output_dir)
        plot_exp_a_lines(recall_matrix, time_matrix, parallelisms, ratios, output_dir)
    else:
        print("\nNo Experiment A data found (exp_a_* test cases)")
    
    if exp_b_has_data:
        print("\nGenerating Experiment B charts...")
        plot_exp_b(k_values, recalls_b, times_b, output_dir, args.num_partitions)
    else:
        print("\nNo Experiment B data found (exp_b_k* test cases)")
    
    # 打印摘要
    print_summary(recall_matrix, time_matrix, parallelisms, ratios,
                  k_values, recalls_b, times_b)

    # 写入 Markdown 摘要（放在 input_dir，避免用户把 output_dir 指到 charts 子目录时丢失上下文）
    try:
        summary_path = Path(args.input_dir) / "EXPERIMENT_SUMMARY.md"
        write_markdown_summary(
            summary_path,
            recall_matrix, time_matrix, sink_wait_matrix, parallelisms, ratios,
            k_values, recalls_b, times_b,
            charts_dir=Path(output_dir),
            num_partitions=args.num_partitions,
        )
    except Exception as e:
        print(f"Warning: Failed to write markdown summary: {e}")
    
    print(f"\nAll charts saved to: {output_dir}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
