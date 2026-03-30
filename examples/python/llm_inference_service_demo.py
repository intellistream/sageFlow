#!/usr/bin/env python3
"""
SageFlow LLM 推理服务链条示例

本示例展示 SageFlow 如何服务于 SAGE 的 LLM 推理链条，包含三个核心场景：

1. 流式 RAG - Query Stream + Document Stream → Similarity Join → Context Builder → LLM Sink
2. 相似查询聚合 - Query Stream → Sliding Window → Aggregate → LLM Sink
3. 会话语义状态维护 - Message Stream → Window → Incremental Centroid → Memory Sink

运行方式:
    cd sageFlow
    python examples/python/llm_inference_service_demo.py
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import numpy as np

try:
    import sage_flow as sf
except ImportError:
    import sys
    from pathlib import Path
    # 添加构建目录到 Python 路径
    build_path = Path(__file__).parent.parent.parent / "build" / "sage_flow"
    if build_path.exists():
        sys.path.insert(0, str(build_path))
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    import sage_flow as sf


# =============================================================================
# 场景 1: 流式 RAG
# =============================================================================
# 将用户查询流与文档知识库流进行实时相似度匹配，
# 匹配结果作为 LLM 的上下文（Context）输入。
#
# Pipeline 架构:
#   Query Stream ─────┐
#                     ├──▶ Similarity Join ──▶ Context Builder ──▶ LLM Sink
#   Document Stream ──┘
# =============================================================================

@dataclass
class RAGContextBuilder:
    """RAG 上下文构建器：收集 Query-Document 匹配对"""
    
    matched_pairs: list[dict[str, Any]] = field(default_factory=list)
    processed_count: int = 0
    
    def on_match(self, uid: int, timestamp: int, combined_embedding: np.ndarray) -> None:
        """接收匹配结果"""
        query_id = uid // 10000
        doc_id = uid % 10000
        self.matched_pairs.append({
            "query_id": query_id,
            "doc_id": doc_id,
            "timestamp": timestamp,
            "context_embedding": combined_embedding.copy(),
            "embedding_norm": float(np.linalg.norm(combined_embedding)),
        })
        self.processed_count += 1
        print(f"  [RAG] Query {query_id} ↔ Doc {doc_id} matched "
              f"(combined dim={len(combined_embedding)})")
    
    def get_context_for_llm(self, query_id: int) -> list[dict]:
        """获取某个查询的所有匹配上下文"""
        return [p for p in self.matched_pairs if p["query_id"] == query_id]


def create_combine_vectors_join():
    """创建向量组合 Join 函数
    
    注意：相似度判断已在 SageFlow C++ 引擎内部完成！
    - BruteForceBaseline::computeSimilarity() 计算相似度
    - 只有满足 threshold 的 pair 才会调用此函数
    
    此函数职责：定义如何组合两个已匹配的向量生成新记录
    
    Args (由 SageFlow 引擎传入，numpy.ndarray 格式):
        l_uid, l_ts, l_vec: 左流记录 (query)
        r_uid, r_ts, r_vec: 右流记录 (document)
    
    Returns:
        (uid, ts, vec) tuple 或 None
        - uid: 新记录的唯一标识
        - ts: 新记录的时间戳
        - vec: numpy.ndarray, 组合后的向量
    """
    def join_func(
        l_uid: int, l_ts: int, l_vec: np.ndarray,
        r_uid: int, r_ts: int, r_vec: np.ndarray
    ) -> tuple[int, int, np.ndarray] | None:
        # 相似度判断已在 C++ 引擎完成，这里直接组合
        # SageFlow 将 VectorRecord 的 data 转换为 numpy.ndarray 传入
        
        # 组合策略 1: 归一化后取平均
        l_norm = np.linalg.norm(l_vec)
        r_norm = np.linalg.norm(r_vec)
        if l_norm < 1e-8 or r_norm < 1e-8:
            # 零向量，使用非零的那个
            combined = l_vec if r_norm < 1e-8 else r_vec
        else:
            combined = ((l_vec / l_norm) + (r_vec / r_norm)) / 2
        
        # 编码 uid：高位=query_id, 低位=doc_id
        combined_uid = l_uid * 10000 + r_uid
        combined_ts = max(l_ts, r_ts)
        
        return (combined_uid, combined_ts, combined.astype(np.float32))
    
    return join_func


def run_streaming_rag_demo():
    """
    场景 1：流式 RAG 演示
    
    核心价值:
    - 实时检索：用户查询立即匹配最相关文档
    - 增量索引：新文档自动加入匹配候选集
    - 上下文新鲜度：始终使用最新的语义匹配结果
    """
    print("\n" + "=" * 70)
    print("场景 1：流式 RAG (Query-Document Similarity Join)")
    print("=" * 70)
    print("Pipeline: Query Stream + Doc Stream → Join → Context → LLM Sink")
    print("-" * 70 + "\n")
    
    # 创建环境
    env = sf.StreamEnvironment()
    
    # 创建数据源
    query_stream = sf.SimpleStreamSource("user_queries")
    doc_stream = sf.SimpleStreamSource("knowledge_base")
    
    # 上下文构建器
    context_builder = RAGContextBuilder()
    
    dim = 128  # 嵌入维度
    
    # 生成测试数据 (必须在构建 pipeline 前准备好向量)
    np.random.seed(42)
    
    # 添加查询向量（3 个查询）
    print(">>> 注入用户查询:")
    query_vectors = []
    for i in range(3):
        vec = np.random.randn(dim).astype(np.float32)
        vec /= np.linalg.norm(vec)  # 归一化
        query_vectors.append(vec)
        query_stream.addRecord(i, i * 1000, vec)
        print(f"  Query {i}: norm={np.linalg.norm(vec):.4f}")
    
    # 添加文档向量（5 个文档，其中一些与查询相似）
    print("\n>>> 注入知识库文档:")
    for i in range(5):
        if i < 3:
            # 前 3 个文档与对应查询相似（添加小噪声）
            vec = query_vectors[i] + np.random.randn(dim).astype(np.float32) * 0.1
        else:
            # 后 2 个文档随机
            vec = np.random.randn(dim).astype(np.float32)
        vec = vec / np.linalg.norm(vec)
        vec = vec.astype(np.float32)
        doc_stream.addRecord(100 + i, i * 500 + 250, vec)
        print(f"  Doc {100 + i}: norm={np.linalg.norm(vec):.4f}")
    
    # 构建 Pipeline
    # 注意：相似度阈值在 SageFlow C++ 引擎层设置，不在 Python callback 中
    # 由于 pybind11 的限制，SimpleStreamSource 需要使用 setter 方法配置 Join 参数
    query_stream.setJoinMethod("bruteforce_lazy")  # C++ Join 算法
    query_stream.setJoinSimilarityThreshold(0.3)   # 相似度阈值（C++ 引擎过滤）
    
    pipeline = (
        query_stream
        .join(doc_stream, create_combine_vectors_join(), dim=dim, parallelism=1)
        .writeSink(context_builder.on_match, parallelism=1)
    )
    
    # 执行
    print("\n>>> 执行 Pipeline:")
    env.addStream(query_stream)
    env.addStream(doc_stream)
    env.execute()
    
    # 等待异步处理
    time.sleep(1.5)
    
    # 结果统计
    print("\n>>> 结果统计:")
    print(f"  总匹配对数: {context_builder.processed_count}")
    for qid in range(3):
        ctx = context_builder.get_context_for_llm(qid)
        print(f"  Query {qid} 匹配文档数: {len(ctx)}")
        for c in ctx:
            print(f"    - Doc {c['doc_id']}, embedding_norm={c['embedding_norm']:.4f}")
    
    return context_builder


# =============================================================================
# 场景 2: 相似查询聚合
# =============================================================================
# 通过滑动窗口检测语义相似的查询，聚合后统一调用 LLM，减少重复计算。
#
# Pipeline 架构:
#   Query Stream ──▶ Sliding Window ──▶ Aggregate (Avg) ──▶ LLM Sink
# =============================================================================

@dataclass
class QueryAggregator:
    """查询聚合器：收集窗口内的聚合结果"""
    
    aggregated_windows: list[dict[str, Any]] = field(default_factory=list)
    llm_call_count: int = 0
    original_query_count: int = 0
    
    def on_aggregated(self, window_id: int, timestamp: int, avg_embedding: np.ndarray) -> None:
        """接收聚合后的代表性嵌入"""
        self.aggregated_windows.append({
            "window_id": window_id,
            "timestamp": timestamp,
            "representative_embedding": avg_embedding.copy(),
            "action": "single_llm_call",
        })
        self.llm_call_count += 1
        print(f"  [Aggregated] Window {window_id}: 生成代表向量 "
              f"(dim={len(avg_embedding)}, 可调用一次 LLM)")
    
    def get_savings_ratio(self) -> float:
        """计算节省的 LLM 调用比例"""
        if self.original_query_count == 0:
            return 0.0
        return 1.0 - (self.llm_call_count / self.original_query_count)


def run_query_aggregation_demo():
    """
    场景 2：相似查询聚合演示
    
    核心价值:
    - 降低成本：相似查询只调用一次 LLM
    - 减少延迟：批量处理提高吞吐量
    - 资源优化：避免重复的 embedding 和推理
    
    注意：此示例展示聚合逻辑，实际窗口聚合依赖 SageFlow C++ 实现。
    这里使用 Map 算子模拟在线聚合以展示概念。
    """
    print("\n" + "=" * 70)
    print("场景 2：相似查询聚合 (Sliding Window + Aggregate)")
    print("=" * 70)
    print("Pipeline: Query Stream → Window → Aggregate → LLM Sink")
    print("-" * 70 + "\n")
    
    # 创建环境
    env = sf.StreamEnvironment()
    query_stream = sf.SimpleStreamSource("queries")
    
    dim = 128
    
    # 在线聚合状态
    class OnlineAggregator:
        def __init__(self, window_size_ms: int = 5000):
            self.window_size = window_size_ms
            self.current_window: list[np.ndarray] = []
            self.current_window_start = 0
            self.aggregated_count = 0
            self.original_count = 0
            self.results: list[dict] = []
        
        def process(self, uid: int, ts: int, vec: np.ndarray) -> np.ndarray | None:
            self.original_count += 1
            
            # 检查是否需要触发新窗口
            if ts >= self.current_window_start + self.window_size and self.current_window:
                # 输出当前窗口的聚合结果
                avg_vec = np.mean(self.current_window, axis=0)
                self.results.append({
                    "window_id": self.aggregated_count,
                    "query_count": len(self.current_window),
                    "representative": avg_vec,
                })
                print(f"  [Aggregated] Window {self.aggregated_count}: "
                      f"{len(self.current_window)} queries → 1 LLM call")
                self.aggregated_count += 1
                self.current_window = []
                self.current_window_start = ts
            
            self.current_window.append(vec.copy())
            return vec
    
    aggregator = OnlineAggregator(window_size_ms=3000)
    
    # 使用 Map 实现在线聚合
    pipeline = (
        query_stream
        .map(lambda uid, ts, vec: aggregator.process(uid, ts, vec), parallelism=1)
        .writeSink(lambda uid, ts, vec: None, parallelism=1)  # 空 sink
    )
    
    # 模拟相似查询到达
    print(">>> 模拟相似查询到达 (同一主题的变体):")
    np.random.seed(123)
    
    num_queries = 10
    
    # 生成一个基础向量，所有查询都是它的噪声变体
    base_embedding = np.random.randn(dim).astype(np.float32)
    base_embedding /= np.linalg.norm(base_embedding)
    
    for i in range(num_queries):
        # 添加小噪声，模拟同一主题的不同表述
        noise = np.random.randn(dim).astype(np.float32) * 0.1
        query_vec = base_embedding + noise
        query_vec = (query_vec / np.linalg.norm(query_vec)).astype(np.float32)
        
        ts = i * 800  # 800ms 间隔
        query_stream.addRecord(i, ts, query_vec)
        sim = np.dot(query_vec, base_embedding)
        print(f"  Query {i}: ts={ts}ms, 与基准相似度={sim:.4f}")
    
    # 执行
    print("\n>>> 执行 Pipeline:")
    env.addStream(query_stream)
    env.execute()
    
    time.sleep(1.5)
    
    # 处理最后一个窗口
    if aggregator.current_window:
        avg_vec = np.mean(aggregator.current_window, axis=0)
        aggregator.results.append({
            "window_id": aggregator.aggregated_count,
            "query_count": len(aggregator.current_window),
            "representative": avg_vec,
        })
        print(f"  [Aggregated] Window {aggregator.aggregated_count}: "
              f"{len(aggregator.current_window)} queries → 1 LLM call (final)")
        aggregator.aggregated_count += 1
    
    # 结果统计
    print("\n>>> 结果统计:")
    print(f"  原始查询数: {aggregator.original_count}")
    print(f"  聚合窗口数 (LLM 调用次数): {aggregator.aggregated_count}")
    if aggregator.original_count > 0:
        savings = 1.0 - (aggregator.aggregated_count / aggregator.original_count)
        print(f"  节省比例: {savings:.1%}")
    
    return aggregator


# =============================================================================
# 场景 3: 会话语义状态维护
# =============================================================================
# 维护对话历史的增量语义质心，用于长期记忆召回和会话主题追踪。
#
# Pipeline 架构:
#   Message Stream ──▶ Window ──▶ Incremental Centroid ──▶ Memory Sink
# =============================================================================

@dataclass
class SessionMemoryStore:
    """会话记忆存储：维护每个会话的语义状态"""
    
    session_centroids: dict[int, np.ndarray] = field(default_factory=dict)
    message_counts: dict[int, int] = field(default_factory=dict)
    update_history: list[dict[str, Any]] = field(default_factory=list)
    
    def update_centroid(self, session_id: int, new_embedding: np.ndarray) -> np.ndarray:
        """
        增量更新质心
        
        公式: centroid_new = (n * centroid_old + embedding_new) / (n + 1)
        
        这是在线平均算法，避免存储所有历史消息。
        """
        if session_id not in self.session_centroids:
            self.session_centroids[session_id] = new_embedding.copy()
            self.message_counts[session_id] = 1
        else:
            n = self.message_counts[session_id]
            old_centroid = self.session_centroids[session_id]
            # 增量质心更新
            new_centroid = (n * old_centroid + new_embedding) / (n + 1)
            self.session_centroids[session_id] = new_centroid
            self.message_counts[session_id] = n + 1
        
        self.update_history.append({
            "session_id": session_id,
            "message_count": self.message_counts[session_id],
            "centroid_norm": float(np.linalg.norm(self.session_centroids[session_id])),
        })
        
        return self.session_centroids[session_id]
    
    def query_similar_sessions(
        self, query_embedding: np.ndarray, top_k: int = 5
    ) -> list[tuple[int, float]]:
        """查找语义最相似的历史会话"""
        if not self.session_centroids:
            return []
        
        similarities = []
        query_norm = np.linalg.norm(query_embedding)
        
        for sid, centroid in self.session_centroids.items():
            centroid_norm = np.linalg.norm(centroid)
            if query_norm < 1e-8 or centroid_norm < 1e-8:
                continue
            sim = np.dot(query_embedding, centroid) / (query_norm * centroid_norm)
            similarities.append((sid, float(sim)))
        
        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:top_k]


def run_session_memory_demo():
    """
    场景 3：会话语义状态维护演示
    
    核心价值:
    - 增量计算：不需要重新计算全部历史
    - 语义压缩：将长对话压缩为代表性向量
    - 记忆检索：支持基于语义的历史会话召回
    """
    print("\n" + "=" * 70)
    print("场景 3：会话语义状态维护 (Incremental Centroid)")
    print("=" * 70)
    print("Pipeline: Message Stream → Window → Centroid Update → Memory Sink")
    print("-" * 70 + "\n")
    
    # 创建环境和存储
    env = sf.StreamEnvironment()
    message_stream = sf.SimpleStreamSource("messages")
    memory_store = SessionMemoryStore()
    
    dim = 128
    
    def on_message(uid: int, ts: int, embedding: np.ndarray) -> None:
        """处理消息并更新会话质心"""
        session_id = uid // 1000
        message_id = uid % 1000
        
        new_centroid = memory_store.update_centroid(session_id, embedding)
        msg_count = memory_store.message_counts[session_id]
        
        print(f"  [Session {session_id}] Msg {message_id}: "
              f"质心更新 (消息数={msg_count}, "
              f"centroid_norm={np.linalg.norm(new_centroid):.4f})")
    
    # 构建 Pipeline - 直接使用 Sink 处理
    pipeline = (
        message_stream
        .writeSink(on_message, parallelism=1)
    )
    
    # 模拟多会话消息
    print(">>> 模拟多会话消息到达:")
    np.random.seed(456)
    
    num_sessions = 3
    msgs_per_session = 5
    session_bases = {}  # 每个会话的基础向量（代表主题）
    
    # 先注入所有数据
    for session_id in range(num_sessions):
        # 每个会话有自己的主题向量
        session_bases[session_id] = np.random.randn(dim).astype(np.float32)
        session_bases[session_id] /= np.linalg.norm(session_bases[session_id])
        print(f"\n  Session {session_id} 主题向量已初始化")
        
        for msg_id in range(msgs_per_session):
            uid = session_id * 1000 + msg_id
            ts = session_id * 10000 + msg_id * 2000  # 不同会话不同时间段
            
            # 同一会话的消息围绕主题向量
            noise = np.random.randn(dim).astype(np.float32) * 0.2
            vec = session_bases[session_id] + noise
            vec = (vec / np.linalg.norm(vec)).astype(np.float32)
            
            message_stream.addRecord(uid, ts, vec)
    
    # 执行
    print("\n>>> 执行 Pipeline:")
    env.addStream(message_stream)
    env.execute()
    
    time.sleep(1.0)
    
    # 结果统计
    print("\n>>> 会话状态统计:")
    for sid in range(num_sessions):
        if sid in memory_store.session_centroids:
            centroid = memory_store.session_centroids[sid]
            base = session_bases[sid]
            sim = np.dot(centroid, base) / (np.linalg.norm(centroid) * np.linalg.norm(base))
            print(f"  Session {sid}: 消息数={memory_store.message_counts[sid]}, "
                  f"质心与主题相似度={sim:.4f}")
    
    # 演示会话检索
    print("\n>>> 演示语义会话检索:")
    # 使用 Session 0 的主题向量作为查询
    query = session_bases[0] + np.random.randn(dim).astype(np.float32) * 0.1
    query = query.astype(np.float32)
    similar = memory_store.query_similar_sessions(query, top_k=3)
    print(f"  查询向量（接近 Session 0 主题）的最相似会话:")
    for sid, sim in similar:
        print(f"    - Session {sid}: similarity={sim:.4f}")
    
    return memory_store


# =============================================================================
# 主程序
# =============================================================================

def main():
    """运行所有 LLM 推理链条示例"""
    print("\n" + "#" * 70)
    print("#" + " " * 18 + "SageFlow LLM 推理服务链条示例" + " " * 17 + "#")
    print("#" * 70)
    
    # 检查 API 可用性
    print("\n[Setup] 检查 SageFlow API...")
    try:
        stream_methods = [m for m in dir(sf.Stream) if not m.startswith('_')]
        print(f"[Setup] Stream 可用方法: {stream_methods[:5]}...")
        print("[Setup] ✓ SageFlow API 正常")
    except Exception as e:
        print(f"[Setup] ✗ SageFlow API 不可用: {e}")
        print("[Setup] 请先构建 SageFlow: cmake -B build && cmake --build build")
        return
    
    # 运行三个场景
    results = {}
    
    try:
        results["rag"] = run_streaming_rag_demo()
    except Exception as e:
        print(f"\n[Error] 场景 1 失败: {e}")
        import traceback
        traceback.print_exc()
    
    try:
        results["aggregation"] = run_query_aggregation_demo()
    except Exception as e:
        print(f"\n[Error] 场景 2 失败: {e}")
        import traceback
        traceback.print_exc()
    
    try:
        results["memory"] = run_session_memory_demo()
    except Exception as e:
        print(f"\n[Error] 场景 3 失败: {e}")
        import traceback
        traceback.print_exc()
    
    # 总结
    print("\n" + "#" * 70)
    print("#" + " " * 24 + "示例运行完成!" + " " * 25 + "#")
    print("#" * 70)
    
    print("\n>>> 场景总结:")
    print("""
    ┌─────────────────────────────────────────────────────────────────┐
    │ 场景 1: 流式 RAG                                                │
    │   • Query + Document 流实时 Join                                │
    │   • 为 LLM 提供动态上下文                                        │
    │   • 适用于：实时问答、知识检索                                    │
    ├─────────────────────────────────────────────────────────────────┤
    │ 场景 2: 相似查询聚合                                             │
    │   • 滑动窗口 + 平均聚合                                          │
    │   • 减少重复 LLM 调用                                            │
    │   • 适用于：高并发查询去重、成本优化                              │
    ├─────────────────────────────────────────────────────────────────┤
    │ 场景 3: 会话语义状态                                             │
    │   • 增量质心维护                                                 │
    │   • 支持语义会话检索                                             │
    │   • 适用于：长期记忆、会话管理、主题追踪                          │
    └─────────────────────────────────────────────────────────────────┘
    """)
    
    return results


if __name__ == "__main__":
    main()
