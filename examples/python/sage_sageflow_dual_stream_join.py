#!/usr/bin/env python3
"""
SAGE + SageFlow 双流 Join Pipeline 示例
=========================================

场景：流式 RAG - 实时匹配用户查询与知识库文档

架构:
    Query Stream (SAGE SourceFunction) ────┐
                                           ├──> SageFlow Join (C++) ──> Context Builder
    Document Stream (SAGE SourceFunction) ─┘

数据流:
    1. Query Stream: 用户查询 → Embedding → 向量
    2. Document Stream: 知识库文档 → Embedding → 向量 (模拟 NeuroMem)
    3. SageFlow Join: 向量相似度匹配 (C++ 高性能引擎)
    4. Context Builder: 组装 RAG 上下文

运行方式:
    cd sageFlow
    LD_LIBRARY_PATH=build/lib:$LD_LIBRARY_PATH python examples/python/sage_sageflow_dual_stream_join.py

依赖:
    - SAGE (sage-kernel, sage-common)
    - SageFlow (C++ bindings)
    - numpy
"""

import sys
import time
import queue
import threading
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any

import numpy as np

# 添加 SAGE 路径
SAGE_ROOT = Path(__file__).parent.parent.parent.parent / "SAGE"
sys.path.insert(0, str(SAGE_ROOT / "packages" / "sage-kernel" / "src"))
sys.path.insert(0, str(SAGE_ROOT / "packages" / "sage-common" / "src"))

# 添加 SageFlow 路径
SAGEFLOW_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(SAGEFLOW_ROOT))

# ============================================================================
# 导入 SAGE 组件
# ============================================================================
from sage.common.core.functions.source_function import SourceFunction
from sage.common.core.functions.map_function import MapFunction
from sage.common.core.functions.sink_function import SinkFunction
from sage.common.core.functions.comap_function import BaseCoMapFunction
from sage.kernel.api.local_environment import LocalEnvironment

# 导入 SageFlow
try:
    import sage_flow as sf
    print("✓ SageFlow C++ 绑定导入成功")
except ImportError as e:
    print(f"✗ SageFlow 导入失败: {e}")
    print("\n请确保设置了 LD_LIBRARY_PATH:")
    print("  LD_LIBRARY_PATH=build/lib:$LD_LIBRARY_PATH python ...")
    sys.exit(1)


# ============================================================================
# 数据结构
# ============================================================================
@dataclass
class Query:
    """用户查询"""
    id: int
    text: str
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))
    embedding: np.ndarray | None = None


@dataclass  
class Document:
    """知识库文档"""
    id: int
    title: str
    content: str
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))
    embedding: np.ndarray | None = None


@dataclass
class RAGContext:
    """RAG 上下文结果"""
    query_id: int
    query_text: str
    matched_docs: list[dict] = field(default_factory=list)
    context_text: str = ""


# ============================================================================
# 模拟 Embedding 函数 (实际应用中替换为真实模型)
# ============================================================================
class SimpleEmbedder:
    """简单的 Embedding 模拟器 (用于演示)"""
    
    def __init__(self, dim: int = 128, seed: int = 42):
        self.dim = dim
        self.rng = np.random.RandomState(seed)
        # 缓存词向量
        self._word_vectors: dict[str, np.ndarray] = {}
    
    def _get_word_vector(self, word: str) -> np.ndarray:
        """获取词向量 (基于哈希的确定性向量)"""
        if word not in self._word_vectors:
            # 使用词的哈希作为随机种子，确保相同词产生相同向量
            word_seed = hash(word) % (2**31)
            rng = np.random.RandomState(word_seed)
            self._word_vectors[word] = rng.randn(self.dim).astype(np.float32)
        return self._word_vectors[word]
    
    def embed(self, text: str) -> np.ndarray:
        """计算文本的 Embedding (词向量平均)"""
        words = text.lower().split()
        if not words:
            return np.zeros(self.dim, dtype=np.float32)
        
        # 计算词向量的平均
        vectors = [self._get_word_vector(w) for w in words]
        embedding = np.mean(vectors, axis=0).astype(np.float32)
        
        # 归一化
        norm = np.linalg.norm(embedding)
        if norm > 0:
            embedding = embedding / norm
        
        return embedding


# 全局 Embedder 实例
EMBEDDER = SimpleEmbedder(dim=128)


# ============================================================================
# SAGE Source Functions
# ============================================================================
class QuerySourceFunction(SourceFunction):
    """
    SAGE Source: 生成用户查询流
    
    模拟实时用户查询输入
    """
    
    def __init__(self, queries: list[dict]):
        """
        Args:
            queries: 查询列表 [{"id": 1, "text": "..."}]
        """
        super().__init__()
        self.queries = queries
        self.index = 0
        self._exhausted = False
    
    def execute(self, data=None) -> Query | None:
        """生成下一个查询"""
        if self.index >= len(self.queries):
            if not self._exhausted:
                self._exhausted = True
                print(f"  [QuerySource] 已发送所有 {len(self.queries)} 个查询")
            return None
        
        q = self.queries[self.index]
        query = Query(
            id=q["id"],
            text=q["text"],
            timestamp=int(time.time() * 1000),
        )
        self.index += 1
        print(f"  [QuerySource] 发送查询 {query.id}: '{query.text}'")
        return query


class DocumentSourceFunction(SourceFunction):
    """
    SAGE Source: 知识库文档流
    
    模拟 NeuroMem 内存系统提供的文档流
    实际应用中可以连接到真正的 NeuroMem VDB
    """
    
    def __init__(self, documents: list[dict]):
        """
        Args:
            documents: 文档列表 [{"id": 1, "title": "...", "content": "..."}]
        """
        super().__init__()
        self.documents = documents
        self.index = 0
        self._exhausted = False
    
    def execute(self, data=None) -> Document | None:
        """生成下一个文档"""
        if self.index >= len(self.documents):
            if not self._exhausted:
                self._exhausted = True
                print(f"  [DocSource] 已发送所有 {len(self.documents)} 个文档")
            return None
        
        d = self.documents[self.index]
        doc = Document(
            id=d["id"],
            title=d["title"],
            content=d["content"],
            timestamp=int(time.time() * 1000),
        )
        self.index += 1
        print(f"  [DocSource] 发送文档 {doc.id}: '{doc.title}'")
        return doc


# ============================================================================
# SAGE Map Functions
# ============================================================================
class QueryEmbeddingFunction(MapFunction):
    """SAGE Map: 计算查询的 Embedding"""
    
    def execute(self, query: Query) -> Query:
        query.embedding = EMBEDDER.embed(query.text)
        return query


class DocumentEmbeddingFunction(MapFunction):
    """SAGE Map: 计算文档的 Embedding"""
    
    def execute(self, doc: Document) -> Document:
        # 使用 title + content 作为文档表示
        text = f"{doc.title} {doc.content}"
        doc.embedding = EMBEDDER.embed(text)
        return doc


# ============================================================================
# SageFlow Join Operator (作为 SAGE CoMapFunction)
# ============================================================================
class SageFlowJoinCoMap(BaseCoMapFunction):
    """
    SAGE CoMapFunction: 包装 SageFlow Join Pipeline
    
    接收两条流:
    - map0: Query 流 (带 embedding)
    - map1: Document 流 (带 embedding)
    
    内部使用 SageFlow C++ 引擎执行向量相似度 Join
    """
    
    def __init__(
        self,
        dim: int = 128,
        similarity_threshold: float = 0.3,
        join_method: str = "bruteforce_lazy",
    ):
        super().__init__()
        self.dim = dim
        self.similarity_threshold = similarity_threshold
        self.join_method = join_method
        
        # SageFlow 组件
        self._sf_env: sf.StreamEnvironment | None = None
        self._query_source: sf.SimpleStreamSource | None = None
        self._doc_source: sf.SimpleStreamSource | None = None
        self._initialized = False
        
        # 结果收集
        self._result_queue: queue.Queue = queue.Queue()
        self._lock = threading.Lock()
        
        # 存储待匹配的数据
        self._pending_queries: dict[int, Query] = {}
        self._pending_docs: dict[int, Document] = {}
        
    def _init_sageflow(self):
        """延迟初始化 SageFlow Pipeline"""
        if self._initialized:
            return
            
        print("  [SageFlowJoin] 初始化 C++ 引擎...")
        
        self._sf_env = sf.StreamEnvironment()
        self._query_source = sf.SimpleStreamSource("query_stream")
        self._doc_source = sf.SimpleStreamSource("doc_stream")
        
        # 配置 Join
        self._query_source.setJoinMethod(self.join_method)
        self._query_source.setJoinSimilarityThreshold(self.similarity_threshold)
        
        # 定义 Join 回调
        def join_callback(q_uid, q_ts, q_vec, d_uid, d_ts, d_vec):
            """SageFlow Join 回调"""
            similarity = float(np.dot(q_vec, d_vec))
            self._result_queue.put({
                "query_id": int(q_uid),
                "doc_id": int(d_uid),
                "similarity": similarity,
            })
            # 返回合并向量
            combined_uid = int(q_uid) * 10000 + int(d_uid)
            combined_ts = max(int(q_ts), int(d_ts))
            combined_vec = ((q_vec + d_vec) / 2).astype(np.float32)
            return (combined_uid, combined_ts, combined_vec)
        
        # 定义 Sink 回调 (空操作，结果已在 join_callback 收集)
        def sink_callback(uid, ts, vec):
            pass
        
        # 构建 Pipeline
        pipeline = (
            self._query_source
            .join(self._doc_source, join_callback, self.dim, 1)
            .writeSink(sink_callback, 1)
        )
        
        self._sf_env.addStream(self._query_source)
        self._sf_env.addStream(self._doc_source)
        
        self._initialized = True
        print("  [SageFlowJoin] C++ 引擎初始化完成")
    
    def map0(self, query: Query) -> RAGContext | None:
        """
        处理 Query 流
        
        将查询向量送入 SageFlow，返回匹配的文档上下文
        """
        with self._lock:
            self._init_sageflow()
        
        if query.embedding is None:
            print(f"  [SageFlowJoin] 警告: Query {query.id} 没有 embedding")
            return None
        
        # 存储查询信息
        self._pending_queries[query.id] = query
        
        # 将查询向量送入 SageFlow
        self._query_source.addRecord(query.id, query.timestamp, query.embedding)
        
        # 执行 SageFlow
        try:
            self._sf_env.execute()
        except Exception as e:
            print(f"  [SageFlowJoin] 执行错误: {e}")
        
        # 等待结果
        time.sleep(0.1)
        
        # 收集匹配结果
        matches = []
        while not self._result_queue.empty():
            try:
                match = self._result_queue.get_nowait()
                if match["query_id"] == query.id:
                    matches.append(match)
            except queue.Empty:
                break
        
        # 构建 RAG 上下文
        matched_docs = []
        for m in sorted(matches, key=lambda x: -x["similarity"]):
            doc = self._pending_docs.get(m["doc_id"])
            if doc:
                matched_docs.append({
                    "id": doc.id,
                    "title": doc.title,
                    "content": doc.content,
                    "similarity": m["similarity"],
                })
        
        # 构建上下文文本
        context_parts = []
        for d in matched_docs[:3]:  # 取 Top-3
            context_parts.append(f"[{d['title']}] {d['content']}")
        
        result = RAGContext(
            query_id=query.id,
            query_text=query.text,
            matched_docs=matched_docs,
            context_text="\n\n".join(context_parts),
        )
        
        if matched_docs:
            print(f"  [SageFlowJoin] Query {query.id} 匹配到 {len(matched_docs)} 个文档")
        
        return result
    
    def map1(self, doc: Document) -> None:
        """
        处理 Document 流
        
        将文档向量索引到 SageFlow
        """
        with self._lock:
            self._init_sageflow()
        
        if doc.embedding is None:
            print(f"  [SageFlowJoin] 警告: Document {doc.id} 没有 embedding")
            return None
        
        # 存储文档信息
        self._pending_docs[doc.id] = doc
        
        # 将文档向量送入 SageFlow
        self._doc_source.addRecord(doc.id, doc.timestamp, doc.embedding)
        
        return None  # 文档流不直接产生输出


# ============================================================================
# SAGE Sink Function
# ============================================================================
class RAGContextSink(SinkFunction):
    """SAGE Sink: 输出 RAG 上下文结果"""
    
    def __init__(self):
        super().__init__()
        self.results: list[RAGContext] = []
    
    def execute(self, data: Any) -> None:
        if data is None:
            return
            
        if isinstance(data, RAGContext):
            self.results.append(data)
            print(f"\n{'='*60}")
            print(f"RAG 结果 - Query {data.query_id}: '{data.query_text}'")
            print("-" * 60)
            if data.matched_docs:
                for i, doc in enumerate(data.matched_docs[:3], 1):
                    print(f"  {i}. [{doc['title']}] (相似度: {doc['similarity']:.4f})")
                    print(f"     {doc['content'][:100]}...")
                print("-" * 60)
                print(f"上下文:\n{data.context_text[:200]}...")
            else:
                print("  没有匹配的文档")
            print("=" * 60)


# ============================================================================
# 主程序
# ============================================================================
def main():
    print("\n" + "#" * 70)
    print("#" + " " * 15 + "SAGE + SageFlow 双流 Join 演示" + " " * 15 + "#")
    print("#" * 70)
    
    # -------------------------------------------------------------------------
    # 准备测试数据
    # -------------------------------------------------------------------------
    print("\n[1] 准备测试数据")
    
    # 知识库文档 (模拟 NeuroMem 提供)
    documents = [
        {
            "id": 1001,
            "title": "Python 基础教程",
            "content": "Python 是一种高级编程语言，具有简洁的语法和丰富的标准库。适合初学者学习编程。",
        },
        {
            "id": 1002,
            "title": "机器学习入门",
            "content": "机器学习是人工智能的一个分支，通过数据训练模型来进行预测和决策。常用算法包括线性回归、决策树等。",
        },
        {
            "id": 1003,
            "title": "深度学习框架对比",
            "content": "PyTorch 和 TensorFlow 是最流行的深度学习框架。PyTorch 更灵活，TensorFlow 更适合生产部署。",
        },
        {
            "id": 1004,
            "title": "向量数据库简介",
            "content": "向量数据库专门用于存储和检索高维向量数据，支持相似度搜索。常见的有 Milvus、Pinecone 等。",
        },
        {
            "id": 1005,
            "title": "RAG 技术详解",
            "content": "RAG (Retrieval-Augmented Generation) 结合检索和生成，先从知识库检索相关文档，再用于增强大模型生成。",
        },
    ]
    
    # 用户查询
    queries = [
        {"id": 1, "text": "如何学习 Python 编程"},
        {"id": 2, "text": "深度学习用什么框架好 PyTorch TensorFlow"},
        {"id": 3, "text": "什么是 RAG 检索增强生成"},
    ]
    
    print(f"  文档数: {len(documents)}")
    print(f"  查询数: {len(queries)}")
    
    # -------------------------------------------------------------------------
    # 方案一：使用纯 SageFlow 实现双流 Join (不依赖 SAGE Kernel)
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("[2] 纯 SageFlow 双流 Join 演示")
    print("=" * 70)
    
    # 创建 SageFlow 环境
    sf_env = sf.StreamEnvironment()
    
    # 创建两个数据源
    query_source = sf.SimpleStreamSource("queries")
    doc_source = sf.SimpleStreamSource("documents")
    
    dim = 128
    
    # 配置 Join
    query_source.setJoinMethod("bruteforce_lazy")
    query_source.setJoinSimilarityThreshold(0.3)
    
    # 预计算 Embedding 并添加到数据源
    print("\n  [添加文档向量]")
    doc_map = {}  # 存储文档信息用于结果展示
    base_ts = int(time.time() * 1000)
    
    for i, d in enumerate(documents):
        text = f"{d['title']} {d['content']}"
        embedding = EMBEDDER.embed(text)
        doc_source.addRecord(d["id"], base_ts + i * 10, embedding)
        doc_map[d["id"]] = d
        print(f"    文档 {d['id']}: {d['title']}")
    
    print("\n  [添加查询向量]")
    query_map = {}  # 存储查询信息
    for i, q in enumerate(queries):
        embedding = EMBEDDER.embed(q["text"])
        query_source.addRecord(q["id"], base_ts + 1000 + i * 100, embedding)
        query_map[q["id"]] = q
        print(f"    查询 {q['id']}: {q['text']}")
    
    # Join 结果收集
    join_results = []
    
    def on_join(q_uid, q_ts, q_vec, d_uid, d_ts, d_vec):
        """Join 回调"""
        similarity = float(np.dot(q_vec, d_vec))
        join_results.append({
            "query_id": int(q_uid),
            "doc_id": int(d_uid),
            "similarity": similarity,
        })
        # 返回合并结果
        combined_uid = int(q_uid) * 10000 + int(d_uid)
        combined_ts = max(int(q_ts), int(d_ts))
        combined_vec = ((q_vec + d_vec) / 2).astype(np.float32)
        return (combined_uid, combined_ts, combined_vec)
    
    def on_sink(uid, ts, vec):
        pass  # 结果已在 on_join 收集
    
    # 构建 Pipeline
    print("\n  [构建 SageFlow Pipeline]")
    pipeline = (
        query_source
        .join(doc_source, on_join, dim, 1)
        .writeSink(on_sink, 1)
    )
    
    sf_env.addStream(query_source)
    sf_env.addStream(doc_source)
    
    # 执行
    print("\n  [执行 Join]")
    print("  " + "-" * 50)
    sf_env.execute()
    print("  " + "-" * 50)
    
    # 等待结果
    time.sleep(0.5)
    
    # 按查询分组并展示结果
    print(f"\n  [Join 结果统计]")
    print(f"    总匹配数: {len(join_results)}")
    
    # 按查询分组
    by_query = {}
    for r in join_results:
        qid = r["query_id"]
        if qid not in by_query:
            by_query[qid] = []
        by_query[qid].append(r)
    
    # 展示每个查询的结果
    print("\n" + "=" * 70)
    print("[3] RAG 上下文结果")
    print("=" * 70)
    
    for qid in sorted(by_query.keys()):
        matches = sorted(by_query[qid], key=lambda x: -x["similarity"])
        query = query_map.get(qid, {"text": "Unknown"})
        
        print(f"\n查询 {qid}: '{query['text']}'")
        print("-" * 50)
        
        # 显示 Top-3 匹配文档
        for i, m in enumerate(matches[:3], 1):
            doc = doc_map.get(m["doc_id"], {"title": "Unknown", "content": ""})
            print(f"  {i}. [{doc['title']}] (相似度: {m['similarity']:.4f})")
            print(f"     {doc['content'][:80]}...")
        
        # 构建上下文
        context_parts = []
        for m in matches[:3]:
            doc = doc_map.get(m["doc_id"])
            if doc:
                context_parts.append(f"[{doc['title']}]\n{doc['content']}")
        
        print(f"\n  📝 RAG 上下文:")
        print("  " + "-" * 46)
        for part in context_parts:
            print(f"  {part[:100]}...")
        print("  " + "-" * 46)
    
    print("\n" + "=" * 70)
    print("✅ 演示完成!")
    print("=" * 70)
    print("""
总结:
1. Query Stream 和 Document Stream 是两条独立的数据流
2. SageFlow C++ 引擎执行向量相似度 Join
3. Join 结果按相似度排序，取 Top-K 作为 RAG 上下文
4. 上下文可以送入 LLM 进行增强生成

实际应用中:
- Document Stream 可以连接 NeuroMem VDB 提供实时文档流
- Query Stream 来自用户实时输入
- Embedding 使用真实的模型 (BGE, OpenAI, etc.)
- 输出送入 LLM 进行回答生成
""")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
