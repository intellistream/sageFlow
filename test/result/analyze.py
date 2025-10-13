import argparse
from pathlib import Path
import urllib.request
import os

import matplotlib
# 必须在导入 pyplot 前设置无显卡后端
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager as fm
from matplotlib.font_manager import FontProperties
from matplotlib.patches import Patch
import pandas as pd
import numpy as np

PARALLELISM_ORDER = [1, 2, 4, 8, 16, 32, 40]
METHOD_ORDER = ["ivf_eager", "bruteforce_eager"]

def _setup_chinese_font():
    """
    优先使用系统已装中文字体；若都没有则自动下载 NotoSansSC-Regular 并注册使用。
    """
    candidates = [
        "Noto Sans CJK SC", "Noto Sans SC", "Source Han Sans CN",
        "WenQuanYi Zen Hei", "SimHei", "Microsoft YaHei", "Sarasa UI SC",
    ]
    available = {f.name for f in fm.fontManager.ttflist}

    chosen_name = None
    for name in candidates:
        if name in available:
            chosen_name = name
            break

    if not chosen_name:
        # 尝试使用项目内字体（若不存在则下载）
        fonts_dir = Path(__file__).parent / ".fonts"
        fonts_dir.mkdir(parents=True, exist_ok=True)
        local_font = fonts_dir / "NotoSansSC-Regular.otf"
        if not local_font.exists():
            url = "https://github.com/googlefonts/noto-cjk/raw/main/Sans/OTF/SimplifiedChinese/NotoSansCJKsc-Regular.otf"
            try:
                # 有些发行版对文件名敏感，下载后统一命名为 NotoSansSC-Regular.otf
                tmp_path = fonts_dir / "NotoSansCJKsc-Regular.otf"
                urllib.request.urlretrieve(url, tmp_path)
                tmp_path.rename(local_font)
            except Exception as e:
                # 下载失败则放弃自动下载
                print(f"[warn] 下载中文字体失败：{e}")

        if local_font.exists():
            try:
                fm.fontManager.addfont(str(local_font))
                chosen_name = FontProperties(fname=str(local_font)).get_name()
            except Exception as e:
                print(f"[warn] 注册本地字体失败：{e}")

    if chosen_name:
        plt.rcParams["font.family"] = "sans-serif"
        plt.rcParams["font.sans-serif"] = [chosen_name] + list(plt.rcParams.get("font.sans-serif", []))
    else:
        print("[warn] 未找到可用中文字体，文本可能显示为方框。建议安装 fonts-noto-cjk。")

    # 解决负号显示为方块
    plt.rcParams["axes.unicode_minus"] = False
    return chosen_name

def make_grouped_bar(pivot_df: pd.DataFrame, ylabel: str, title: str, out_path: Path):
    _setup_chinese_font()
    # 只保留有数据的列（方法）
    pivot_df = pivot_df[[c for c in METHOD_ORDER if c in pivot_df.columns]]

    x = np.arange(len(pivot_df.index))
    n_methods = len(pivot_df.columns)
    width = 0.35 if n_methods == 2 else 0.6 / max(n_methods, 1)
    offsets = (np.arange(n_methods) - (n_methods - 1) / 2.0) * width

    fig, ax = plt.subplots(figsize=(10, 5))
    colors = {
        "ivf_eager": "#1f77b4",         # 蓝
        "bruteforce_eager": "#ff7f0e",  # 橙
    }

    for i, method in enumerate(pivot_df.columns):
        y = pivot_df[method].values
        ax.bar(x + offsets[i], y, width, label=method, color=colors.get(method, None))

    ax.set_xlabel("并行度")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticks(x, [str(p) for p in pivot_df.index])
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

def make_grouped_stacked_100(lock_df: pd.DataFrame, exec_df: pd.DataFrame, title: str, out_path: Path):
    """绘制100%分层柱状图：每个柱子= 有效执行% + 锁等待%（两者相加为100）。

    参数:
      - lock_df: 行为并行度，列为方法，值为锁等待百分比
      - exec_df: 行为并行度，列为方法，值为有效执行百分比
    """
    _setup_chinese_font()

    # 对齐列顺序和索引顺序
    methods = [c for c in METHOD_ORDER if c in lock_df.columns or c in exec_df.columns]
    lock_df = lock_df.reindex(columns=methods)
    exec_df = exec_df.reindex(columns=methods)
    lock_df = lock_df.reindex(PARALLELISM_ORDER).dropna(how="all")
    exec_df = exec_df.reindex(PARALLELISM_ORDER).dropna(how="all")

    x = np.arange(len(lock_df.index))
    n_methods = len(methods)
    width = 0.35 if n_methods == 2 else 0.6 / max(n_methods, 1)
    offsets = (np.arange(n_methods) - (n_methods - 1) / 2.0) * width

    fig, ax = plt.subplots(figsize=(11, 5.5))
    colors = {
        "exec": "#2ca02c",  # 绿色 有效执行
        "lock": "#d62728",  # 红色 锁等待
    }
    hatches = {
        "ivf_eager": "//",
        "bruteforce_eager": "\\\\",
    }

    for i, method in enumerate(methods):
        exec_vals = exec_df[method].values if method in exec_df.columns else np.zeros(len(x))
        lock_vals = lock_df[method].values if method in lock_df.columns else np.zeros(len(x))
        # 底层：有效执行
        ax.bar(
            x + offsets[i], exec_vals, width,
            label=None if i else "有效执行",
            color=colors["exec"],
            edgecolor="#333333", linewidth=0.6, hatch=hatches.get(method, None)
        )
        # 顶层：锁等待
        ax.bar(
            x + offsets[i], lock_vals, width, bottom=exec_vals,
            label=None if i else "锁等待",
            color=colors["lock"],
            edgecolor="#333333", linewidth=0.6, hatch=hatches.get(method, None)
        )

    ax.set_xlabel("并行度")
    ax.set_ylabel("百分比 (%)")
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels([str(p) for p in lock_df.index])
    # 图例：组件（有效执行/锁等待）+ 方法（ivf/bruteforce）
    comp_handles = [
        Patch(facecolor=colors["exec"], edgecolor="#333333", label="有效执行"),
        Patch(facecolor=colors["lock"], edgecolor="#333333", label="锁等待"),
    ]
    legend1 = ax.legend(handles=comp_handles, loc="upper right", title="组成")
    method_handles = []
    method_labels_map = {"ivf_eager": "IVF", "bruteforce_eager": "BruteForce"}
    for m in methods:
        method_handles.append(Patch(facecolor="#dddddd", edgecolor="#333333", hatch=hatches.get(m, None), label=method_labels_map.get(m, m)))
    ax.add_artist(legend1)
    ax.legend(handles=method_handles, loc="upper left", title="方法")
    ax.set_ylim(0, 100)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

def make_grouped_stacked_multi(parts: list, title: str, out_path: Path):
    """绘制按方法分组的多段100%堆叠柱状图。
    parts: 列表[(name, df, color)]，df 形状为 (parallelism x method) 的百分比值（0-100）。
    """
    _setup_chinese_font()

    # 对齐列与索引
    if not parts:
        return
    methods = [c for c in METHOD_ORDER if c in parts[0][1].columns]
    x_index = parts[0][1].index
    for i in range(len(parts)):
        parts[i] = (parts[i][0], parts[i][1].reindex(index=PARALLELISM_ORDER).reindex(columns=methods), parts[i][2])

    x = np.arange(len(PARALLELISM_ORDER))
    n_methods = len(methods)
    width = 0.35 if n_methods == 2 else 0.6 / max(n_methods, 1)
    offsets = (np.arange(n_methods) - (n_methods - 1) / 2.0) * width

    fig, ax = plt.subplots(figsize=(12, 6))
    hatches = {"ivf_eager": "//", "bruteforce_eager": "\\\\"}

    for i, method in enumerate(methods):
        bottom = np.zeros(len(PARALLELISM_ORDER))
        for name, dfp, color in parts:
            vals = dfp[method].values if method in dfp.columns else np.zeros(len(PARALLELISM_ORDER))
            ax.bar(
                x + offsets[i], vals, width, bottom=bottom,
                label=name if (i == 0) else None,
                color=color, edgecolor="#333333", linewidth=0.6, hatch=hatches.get(method, None)
            )
            bottom = bottom + vals

    ax.set_xlabel("并行度")
    ax.set_ylabel("百分比 (%)")
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels([str(p) for p in PARALLELISM_ORDER])
    ax.set_ylim(0, 100)
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    # 图例：阶段 + 方法
    legend1 = ax.legend(loc="upper right", title="阶段")
    ax.add_artist(legend1)
    method_labels_map = {"ivf_eager": "IVF", "bruteforce_eager": "BruteForce"}
    method_handles = [Patch(facecolor="#dddddd", edgecolor="#333333", hatch=hatches.get(m, None), label=method_labels_map.get(m, m)) for m in methods]
    ax.legend(handles=method_handles, loc="upper left", title="方法")

    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

def main():
    parser = argparse.ArgumentParser(description="生成 size=4000 的分组柱状图（耗时/吞吐量）")
    parser.add_argument("--input", "-i", type=str,
                        default="/root/candyFlow_zero/test/result/perf_report.tsv",
                        help="perf_report.tsv 文件路径")
    parser.add_argument("--outdir", "-o", type=str,
                        default="/root/candyFlow_zero/test/result/plots",
                        help="输出图片目录")
    args = parser.parse_args()

    in_path = Path(args.input)
    out_dir = Path(args.outdir)

    if not in_path.exists():
        raise FileNotFoundError(f"未找到输入文件: {in_path}")

    df = pd.read_csv(in_path, sep="\t")

    # 仅保留 size=4000 且并行度在指定集合内的数据
    df_4k = df[(df["size"] == 4000) & (df["parallelism"].isin(PARALLELISM_ORDER))].copy()

    # 只取必要列（含锁等待占比计算需要的列）
    cols = [
        "method", "parallelism", "time_ms", "output_tput_rps",
        "lock_wait_ms", "window_ns", "index_ns", "sim_ns", "candidate_fetch_ns",  # 注意：这里使用 candidate_fetch_ns 作为 candidate_ns
    ]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"缺少列: {missing}")

    if df_4k.empty:
        print("警告：未找到 size=4000 的数据，图表将为空。请确认 perf_report.tsv。")

    # 按并行度排序
    df_4k["parallelism"] = pd.Categorical(df_4k["parallelism"], PARALLELISM_ORDER, ordered=True)

    # 为 time_ms 生成分组柱状图
    piv_time = df_4k.pivot_table(index="parallelism", columns="method", values="time_ms", aggfunc="first")
    piv_time = piv_time.reindex(PARALLELISM_ORDER).dropna(how="all")
    make_grouped_bar(
        piv_time,
        ylabel="耗时 (ms)",
        title="size=4000: 耗时对比（越低越好）",
        out_path=out_dir / "size4000_time_ms.png",
    )

    # 为 output_tput_rps 生成分组柱状图
    piv_tput = df_4k.pivot_table(index="parallelism", columns="method", values="output_tput_rps", aggfunc="first")
    piv_tput = piv_tput.reindex(PARALLELISM_ORDER).dropna(how="all")
    make_grouped_bar(
        piv_tput,
        ylabel="吞吐量 (RPS)",
        title="size=4000: 吞吐量对比（越高越好）",
        out_path=out_dir / "size4000_output_tput_rps.png",
    )

    # 计算 breakdown：
    # compute_ms 采用 join 的所有阶段：window_ns + index_ns + sim_ns + joinF_ns + emit_ns + candidate_fetch_ns (+ 可选 expire_ns)
    def col_or_zero(name: str):
        return df_4k[name].astype(float) if name in df_4k.columns else 0.0
    compute_ms = (
        col_or_zero("window_ns")
        + col_or_zero("index_ns")
        + col_or_zero("sim_ns")
        + col_or_zero("joinF_ns")
        + col_or_zero("emit_ns")
        + col_or_zero("candidate_fetch_ns")
        + col_or_zero("expire_ns")
    ) / 1e6
    with np.errstate(divide="ignore", invalid="ignore"):
        # 用户要求：锁占比 = lock_wait_ms / compute_ms
        lock_wait_pct = np.where(compute_ms > 0, df_4k["lock_wait_ms"].astype(float) / compute_ms * 100.0, np.nan)
    # 100%堆叠图：以 lock/compute 为准
    lock_pct_100 = lock_wait_pct.copy()
    exec_pct_100 = 100.0 - lock_pct_100
    total_with_lock_ms = compute_ms + df_4k["lock_wait_ms"].astype(float)

    df_4k = df_4k.assign(
        compute_ms=compute_ms,
        total_with_lock_ms=total_with_lock_ms,
        lock_wait_pct=lock_wait_pct,
        lock_pct_100=lock_pct_100,
        exec_pct_100=exec_pct_100,
    )

    # 为 锁等待占比（单值）生成分组柱状图（按 lock/compute）
    piv_lock = df_4k.pivot_table(index="parallelism", columns="method", values="lock_wait_pct", aggfunc="first")
    piv_lock = piv_lock.reindex(PARALLELISM_ORDER).dropna(how="all")
    make_grouped_bar(
        piv_lock,
        ylabel="锁等待占比 (%)",
        title="size=4000: 锁等待占比（越低越好）",
        out_path=out_dir / "size4000_lock_wait_pct.png",
    )

    # 100%分层柱状图：锁等待 vs 有效执行
    piv_lock100 = df_4k.pivot_table(index="parallelism", columns="method", values="lock_pct_100", aggfunc="first")
    piv_exec100 = df_4k.pivot_table(index="parallelism", columns="method", values="exec_pct_100", aggfunc="first")
    make_grouped_stacked_100(
        piv_lock100,
        piv_exec100,
        title="size=4000: 锁等待 vs 有效执行（100%堆叠）",
        out_path=out_dir / "size4000_lock_breakdown_pct.png",
    )

    # 生成四阶段（window/index/sim/candidate_fetch）的100%堆叠图
    # 以这四项之和为分母
    denom4 = (
        df_4k["window_ns"].astype(float) + df_4k["index_ns"].astype(float) +
        df_4k["sim_ns"].astype(float) + df_4k["candidate_fetch_ns"].astype(float)
    )
    with np.errstate(divide="ignore", invalid="ignore"):
        w_pct = np.where(denom4 > 0, df_4k["window_ns"].astype(float) / denom4 * 100.0, np.nan)
        i_pct = np.where(denom4 > 0, df_4k["index_ns"].astype(float) / denom4 * 100.0, np.nan)
        s_pct = np.where(denom4 > 0, df_4k["sim_ns"].astype(float) / denom4 * 100.0, np.nan)
        c_pct = np.where(denom4 > 0, df_4k["candidate_fetch_ns"].astype(float) / denom4 * 100.0, np.nan)
    df_4k = df_4k.assign(
        window_pct_4=w_pct, index_pct_4=i_pct, sim_pct_4=s_pct, candidate_fetch_pct_4=c_pct
    )
    piv_w = df_4k.pivot_table(index="parallelism", columns="method", values="window_pct_4", aggfunc="first").reindex(PARALLELISM_ORDER)
    piv_i = df_4k.pivot_table(index="parallelism", columns="method", values="index_pct_4", aggfunc="first").reindex(PARALLELISM_ORDER)
    piv_s = df_4k.pivot_table(index="parallelism", columns="method", values="sim_pct_4", aggfunc="first").reindex(PARALLELISM_ORDER)
    piv_c = df_4k.pivot_table(index="parallelism", columns="method", values="candidate_fetch_pct_4", aggfunc="first").reindex(PARALLELISM_ORDER)

    parts = [
        ("窗口(window)", piv_w, "#1f77b4"),
        ("索引(index)", piv_i, "#ff7f0e"),
        ("相似度(sim)", piv_s, "#2ca02c"),
        ("候选抓取(candidate)", piv_c, "#9467bd"),
    ]
    make_grouped_stacked_multi(
        parts,
        title="size=4000: 计算阶段占比（100%堆叠）",
        out_path=out_dir / "size4000_compute_breakdown_pct.png",
    )

    # 导出汇总
    # 将计算得到的指标追加到汇总
    summary_cols = cols + [
        "compute_ms",
        "total_with_lock_ms",
        "lock_wait_pct",
        "lock_pct_100",
        "exec_pct_100",
    ]
    summary = df_4k.sort_values(["parallelism", "method"])[summary_cols]
    out_dir.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out_dir / "size4000_summary.tsv", sep="\t", index=False)

    print(
        "已生成：\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}".format(
            out_dir / "size4000_time_ms.png",
            out_dir / "size4000_output_tput_rps.png",
            out_dir / "size4000_lock_wait_pct.png",
            out_dir / "size4000_lock_breakdown_pct.png",
            out_dir / "size4000_compute_breakdown_pct.png",
            out_dir / "size4000_summary.tsv",
        )
    )

if __name__ == "__main__":
    main()