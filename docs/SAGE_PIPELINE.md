"""
RAG Pipeline with SageFlow: Incremental Semantic State Maintenance

场景：
- 用户查询流：持续到达的用户问题（embedding 化后的向量）
- 知识库流：动态更新的文档 chunk embeddings
- 目标：实时检索、相似查询聚合、热点追踪

架构：
  User Query Stream ──┐
                      ├──> Similarity Join ──> LLM Context Builder ──> vLLM
  Knowledge Stream ───┘
"""

import sage_flow as sf
from sage.middleware.components.sage_mem import MemoryManager
from sage.common.components.sage_embedding import EmbeddingFactory

# ============================================================
# Pipeline 1: 查询去重与聚合（减少重复 LLM 调用）
# ============================================================
def build_query_dedup_pipeline():
    """
    相似查询聚合：将语义相近的用户问题聚合，复用 LLM 响应

    流程：
    Query Embedding Stream
        -> Window(5s)
        -> SimilarityJoin(self, threshold=0.92)  # 检测重复查询
        -> Aggregate(centroid)                    # 聚合为代表性查询
        -> Sink(LLM inference)
    """
    env = sf.StreamEnvironment()

    # 查询 embedding 流（从 Gateway 接收）
    query_stream = sf.SimpleStreamSource("user_queries")

    # 构建 pipeline
    pipeline = (query_stream
        # 5秒滑动窗口，聚合相似查询
        .window(sf.WindowFunction("query_window",
                                   window_size_ms=5000,
                                   step_ms=1000,
                                   window_type=sf.WindowType.Sliding))
        # 窗口内相似度聚合（去重）
        .aggregate(sf.AggregateFunction("centroid", sf.AggregateType.Avg))
        # 输出到 LLM 推理
        .write_sink(sf.SinkFunction("llm_sink", forward_to_llm))
    )

    env.addStream(pipeline)
    return env


# ============================================================
# Pipeline 2: 流式 RAG 检索（Query-Document Join）
# ============================================================
def build_streaming_rag_pipeline():
    """
    流式 RAG：实时匹配用户查询与知识库文档

    流程：
    Query Stream ────┐
                     ├──> Similarity Join (threshold=0.75) ──> Context Builder
    Document Stream ─┘

    这替代了传统 RAG 的"查询时检索"，实现"流式匹配"
    """
    env = sf.StreamEnvironment()

    # 双流：查询流 + 文档流
    query_stream = sf.SimpleStreamSource("queries")      # 用户查询 embeddings
    doc_stream = sf.SimpleStreamSource("documents")      # 知识库 chunk embeddings

    # 流式相似性 Join（核心：替代传统向量检索）
    rag_pipeline = (query_stream
        .join(
            doc_stream,
            sf.JoinFunction("rag_join", dim=1024),  # BGE-M3 维度
            method="hnsw",           # 使用 HNSW 加速
            threshold=0.75,          # 相似度阈值
            parallelism=4            # 并行度
        )
        # Join 结果：(query, matched_doc) pairs
        .write_sink(sf.SinkFunction("context_builder", build_llm_context))
    )

    env.addStream(rag_pipeline)
    return env


# ============================================================
# Pipeline 3: 会话语义状态追踪（Session Memory）
# ============================================================
def build_session_memory_pipeline():
    """
    会话记忆流：维护多轮对话的增量语义状态

    场景：用户多轮对话中，追踪话题漂移和关键信息

    流程：
    Message Stream
        -> Window(session)           # 会话窗口
        -> IncrementalCentroid       # 计算话题中心
        -> SimilarityFilter          # 过滤离题消息
        -> NeuroMem                  # 写入记忆系统
    """
    env = sf.StreamEnvironment()
    memory = MemoryManager()

    # 对话消息 embedding 流
    message_stream = sf.SimpleStreamSource("session_messages")

    pipeline = (message_stream
        # 会话窗口（按 session_id 分组）
        .window(sf.WindowFunction("session_window",
                                   window_size_ms=300000,  # 5分钟会话
                                   step_ms=60000))
        # 计算会话的语义中心（增量更新）
        .aggregate(sf.AggregateFunction("topic_centroid", sf.AggregateType.Avg))
        # 输出到 NeuroMem 记忆系统
        .write_sink(sf.SinkFunction("memory_sink",
                                     lambda rec: memory.store(rec)))
    )

    env.addStream(pipeline)
    return env


# ============================================================
# Pipeline 4: 热点查询检测（用于缓存预热）
# ============================================================
def build_hotspot_detection_pipeline():
    """
    热点检测：识别高频相似查询，预热 LLM 响应缓存

    流程：
    Query Stream
        -> Window(1min)
        -> SelfJoin(threshold=0.9)   # 检测相似查询对
        -> Count by cluster          # 统计每个簇的查询数
        -> Filter(count > threshold) # 筛选热点
        -> Cache warmup
    """
    env = sf.StreamEnvironment()

    query_stream = sf.SimpleStreamSource("all_queries")

    pipeline = (query_stream
        # 1分钟滚动窗口
        .window(sf.WindowFunction("hotspot_window",
                                   window_size_ms=60000,
                                   step_ms=60000,
                                   window_type=sf.WindowType.Tumbling))
        # 聚合统计
        .aggregate(sf.AggregateFunction("cluster_count", sf.AggregateType.Count))
        # 过滤出高频簇
        .filter(sf.FilterFunction("hotspot_filter",
                                   lambda rec: get_count(rec) > 10))
        # 触发缓存预热
        .write_sink(sf.SinkFunction("cache_warmer", warm_llm_cache))
    )

    env.addStream(pipeline)
    return env


# ============================================================
# 辅助函数
# ============================================================
def forward_to_llm(record):
    """将聚合后的代表性查询发送到 vLLM"""
    import openai
    client = openai.OpenAI(base_url="http://localhost:8001/v1", api_key="dummy")
    # ... 调用 LLM

def build_llm_context(query_rec, doc_rec):
    """构建 LLM 上下文（query + retrieved docs）"""
    context = f"Based on: {doc_rec.text}\n\nQuestion: {query_rec.text}"
    return context

def warm_llm_cache(cluster_centroid):
    """预热 LLM 缓存：为热点查询预生成响应"""
    # ... 预生成响应并缓存