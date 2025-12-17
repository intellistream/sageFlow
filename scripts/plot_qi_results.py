#!/usr/bin/env python3
"""
Plot QI strategy (no Q2) performance results
"""

import matplotlib.pyplot as plt
import numpy as np

# QI Strategy Results (No Q2)
parallelism = [1, 2, 4, 8, 16, 32]

# BruteForce results
bf_time_qi = [10477, 5427, 3414, 3905, 9104, 4846]
bf_recall_qi = [1.000, 0.996, 0.999, 0.997, 0.993, 0.989]

# IVF results  
ivf_time_qi = [6309, 4202, 3131, 3308, 5398, 10337]
ivf_recall_qi = [1.000, 1.000, 0.999, 0.998, 0.995, 0.984]

# Original QIQ Strategy Results (with Q2) - from previous tests
bf_time_qiq = [10822, 16821, 24486, 28000, 26221, 23942]
bf_recall_qiq = [1.000, 1.000, 1.000, 1.000, 1.000, 0.998]

ivf_time_qiq = [9892, 16500, 22590, 27000, 26918, 28102]
ivf_recall_qiq = [1.000, 1.000, 1.000, 1.000, 1.000, 1.000]

# Create figure with 2x2 subplots
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('QI Strategy vs QIQ Strategy Performance Comparison', fontsize=14, fontweight='bold')

# Plot 1: BruteForce Time Comparison
ax1 = axes[0, 0]
x = np.arange(len(parallelism))
width = 0.35
bars1 = ax1.bar(x - width/2, bf_time_qiq, width, label='QIQ (with Q2)', color='#ff7f0e', alpha=0.8)
bars2 = ax1.bar(x + width/2, bf_time_qi, width, label='QI (no Q2)', color='#1f77b4', alpha=0.8)
ax1.set_xlabel('Parallelism')
ax1.set_ylabel('Time (ms)')
ax1.set_title('BruteForce: Execution Time')
ax1.set_xticks(x)
ax1.set_xticklabels(parallelism)
ax1.legend()
ax1.grid(axis='y', alpha=0.3)

# Add speedup annotations
for i, (qiq, qi) in enumerate(zip(bf_time_qiq, bf_time_qi)):
    speedup = qiq / qi
    ax1.annotate(f'{speedup:.1f}x', xy=(i + width/2, qi), ha='center', va='bottom', fontsize=9, color='green')

# Plot 2: IVF Time Comparison
ax2 = axes[0, 1]
bars1 = ax2.bar(x - width/2, ivf_time_qiq, width, label='QIQ (with Q2)', color='#ff7f0e', alpha=0.8)
bars2 = ax2.bar(x + width/2, ivf_time_qi, width, label='QI (no Q2)', color='#1f77b4', alpha=0.8)
ax2.set_xlabel('Parallelism')
ax2.set_ylabel('Time (ms)')
ax2.set_title('IVF: Execution Time')
ax2.set_xticks(x)
ax2.set_xticklabels(parallelism)
ax2.legend()
ax2.grid(axis='y', alpha=0.3)

# Add speedup annotations
for i, (qiq, qi) in enumerate(zip(ivf_time_qiq, ivf_time_qi)):
    speedup = qiq / qi
    ax2.annotate(f'{speedup:.1f}x', xy=(i + width/2, qi), ha='center', va='bottom', fontsize=9, color='green')

# Plot 3: BruteForce Recall Comparison
ax3 = axes[1, 0]
ax3.plot(parallelism, bf_recall_qiq, 'o-', label='QIQ (with Q2)', color='#ff7f0e', linewidth=2, markersize=8)
ax3.plot(parallelism, bf_recall_qi, 's-', label='QI (no Q2)', color='#1f77b4', linewidth=2, markersize=8)
ax3.set_xlabel('Parallelism')
ax3.set_ylabel('Recall')
ax3.set_title('BruteForce: Recall')
ax3.set_ylim(0.95, 1.01)
ax3.legend()
ax3.grid(alpha=0.3)
ax3.axhline(y=0.99, color='red', linestyle='--', alpha=0.5, label='99% threshold')

# Plot 4: IVF Recall Comparison
ax4 = axes[1, 1]
ax4.plot(parallelism, ivf_recall_qiq, 'o-', label='QIQ (with Q2)', color='#ff7f0e', linewidth=2, markersize=8)
ax4.plot(parallelism, ivf_recall_qi, 's-', label='QI (no Q2)', color='#1f77b4', linewidth=2, markersize=8)
ax4.set_xlabel('Parallelism')
ax4.set_ylabel('Recall')
ax4.set_title('IVF: Recall')
ax4.set_ylim(0.95, 1.01)
ax4.legend()
ax4.grid(alpha=0.3)
ax4.axhline(y=0.99, color='red', linestyle='--', alpha=0.5, label='99% threshold')

plt.tight_layout()
plt.savefig('/root/sageFlow/test/result/qi_vs_qiq_comparison.png', dpi=150, bbox_inches='tight')
plt.savefig('/root/sageFlow/test/result/qi_vs_qiq_comparison.pdf', bbox_inches='tight')
print("Saved: /root/sageFlow/test/result/qi_vs_qiq_comparison.png")
print("Saved: /root/sageFlow/test/result/qi_vs_qiq_comparison.pdf")

# Summary table
print("\n" + "="*80)
print("QI Strategy (No Q2) vs QIQ Strategy (With Q2) Summary")
print("="*80)
print(f"\n{'Method':<12} {'Parallelism':<12} {'QIQ Time':<12} {'QI Time':<12} {'Speedup':<10} {'QIQ Recall':<12} {'QI Recall':<12}")
print("-"*80)
for i, p in enumerate(parallelism):
    bf_speedup = bf_time_qiq[i] / bf_time_qi[i]
    print(f"{'BruteForce':<12} {p:<12} {bf_time_qiq[i]:<12} {bf_time_qi[i]:<12} {bf_speedup:<10.2f} {bf_recall_qiq[i]:<12.3f} {bf_recall_qi[i]:<12.3f}")
print("-"*80)
for i, p in enumerate(parallelism):
    ivf_speedup = ivf_time_qiq[i] / ivf_time_qi[i]
    print(f"{'IVF':<12} {p:<12} {ivf_time_qiq[i]:<12} {ivf_time_qi[i]:<12} {ivf_speedup:<10.2f} {ivf_recall_qiq[i]:<12.3f} {ivf_recall_qi[i]:<12.3f}")

print("\n" + "="*80)
print("Key Findings:")
print("="*80)
print(f"- BruteForce p=4: {bf_time_qiq[2]}ms → {bf_time_qi[2]}ms ({bf_time_qiq[2]/bf_time_qi[2]:.1f}x speedup)")
print(f"- IVF p=4: {ivf_time_qiq[2]}ms → {ivf_time_qi[2]}ms ({ivf_time_qiq[2]/ivf_time_qi[2]:.1f}x speedup)")
print(f"- Recall drop at p=4: BruteForce {bf_recall_qiq[2]:.3f}→{bf_recall_qi[2]:.3f}, IVF {ivf_recall_qiq[2]:.3f}→{ivf_recall_qi[2]:.3f}")
print(f"- Best speedup: BruteForce p=4 ({bf_time_qiq[2]/bf_time_qi[2]:.1f}x), IVF p=4 ({ivf_time_qiq[2]/ivf_time_qi[2]:.1f}x)")
