"""
LLM Pipeline Example - Demonstrating SageFlow's full Python API for LLM inference chains.

This example shows how to build a complete streaming pipeline for LLM context:
  Query Stream + Document Stream -> Similarity Join -> Context Builder -> LLM Sink

Features demonstrated:
- filter: Filter records based on custom criteria
- map: Transform vector data
- join: Similarity-based join between query and document streams  
- window: Time-based windowing for state management
- aggregate: Aggregate vectors within windows
- writeSink: Output results to Python callbacks
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import numpy as np

try:
    import sage_flow as sf
except ImportError:
    # For development: try relative import
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    import sage_flow as sf


class LLMContextBuilder:
    """Collects joined query-document pairs for LLM context."""
    
    def __init__(self):
        self.context_pairs: list[dict[str, Any]] = []
        self.processed_count = 0
    
    def on_join_result(self, uid: int, timestamp: int, data: np.ndarray) -> None:
        """Callback for join results."""
        self.context_pairs.append({
            "uid": uid,
            "timestamp": timestamp,
            "embedding": data.copy(),
            "similarity_score": float(np.linalg.norm(data))  # Example metric
        })
        self.processed_count += 1
        print(f"[Context] Received pair uid={uid}, ts={timestamp}, dim={len(data)}")


def create_filter_by_norm(min_norm: float = 0.1) -> Callable:
    """Create a filter function that removes low-norm vectors."""
    def filter_func(uid: int, timestamp: int, data: np.ndarray) -> bool:
        norm = np.linalg.norm(data)
        keep = norm >= min_norm
        if not keep:
            print(f"[Filter] Dropped uid={uid} (norm={norm:.4f} < {min_norm})")
        return keep
    return filter_func


def create_normalize_map() -> Callable:
    """Create a map function that normalizes vectors."""
    def map_func(uid: int, timestamp: int, data: np.ndarray) -> np.ndarray:
        norm = np.linalg.norm(data)
        if norm > 0:
            normalized = data / norm
            print(f"[Map] Normalized uid={uid}, original_norm={norm:.4f}")
            return normalized
        return data
    return map_func


def create_similarity_join() -> Callable:
    """Create a join function that combines similar query-document pairs."""
    def join_func(
        left_uid: int, left_ts: int, left_data: np.ndarray,
        right_uid: int, right_ts: int, right_data: np.ndarray
    ) -> tuple[int, int, np.ndarray] | None:
        # Compute cosine similarity
        dot_product = np.dot(left_data, right_data)
        left_norm = np.linalg.norm(left_data)
        right_norm = np.linalg.norm(right_data)
        
        if left_norm > 0 and right_norm > 0:
            similarity = dot_product / (left_norm * right_norm)
        else:
            similarity = 0.0
        
        # Only emit if similarity is above threshold
        threshold = 0.5
        if similarity >= threshold:
            # Create combined embedding (average of query and document)
            combined = (left_data + right_data) / 2.0
            combined_uid = left_uid * 1000 + right_uid  # Composite ID
            combined_ts = max(left_ts, right_ts)
            print(f"[Join] Matched query={left_uid} with doc={right_uid}, similarity={similarity:.4f}")
            return (combined_uid, combined_ts, combined.astype(np.float32))
        
        return None  # No match
    
    return join_func


def run_basic_pipeline():
    """Run a basic pipeline demonstrating filter -> map -> sink."""
    print("\n" + "="*60)
    print("Basic Pipeline: filter -> map -> sink")
    print("="*60 + "\n")
    
    # Create environment and source
    env = sf.StreamEnvironment()
    source = sf.SimpleStreamSource("query_stream")
    
    # Build pipeline
    results = []
    
    def collect_sink(uid: int, ts: int, data: np.ndarray):
        results.append({"uid": uid, "ts": ts, "data": data.copy()})
        print(f"[Sink] Received uid={uid}, ts={ts}, norm={np.linalg.norm(data):.4f}")
    
    # Chain operators: filter low-norm -> normalize -> collect
    pipeline = (
        source
        .filter(create_filter_by_norm(0.5), parallelism=1)
        .map(create_normalize_map(), parallelism=1)
        .writeSink(collect_sink, parallelism=1)
    )
    
    # Add data
    for i in range(5):
        vec = np.random.randn(4).astype(np.float32)
        vec *= (i + 1) * 0.3  # Vary magnitudes
        source.addRecord(i, i * 100, vec)
        print(f"[Source] Added uid={i}, norm={np.linalg.norm(vec):.4f}")
    
    # Register and execute
    env.addStream(source)
    env.execute()
    
    # Wait for async processing
    time.sleep(1.0)
    
    print(f"\n[Result] Processed {len(results)} records through pipeline")
    return results


def run_join_pipeline():
    """Run a join pipeline demonstrating query-document similarity join."""
    print("\n" + "="*60)
    print("Join Pipeline: query_stream JOIN doc_stream -> context_sink")
    print("="*60 + "\n")
    
    # Create environment
    env = sf.StreamEnvironment()
    
    # Create two streams: queries and documents
    query_source = sf.SimpleStreamSource("query_stream")
    doc_source = sf.SimpleStreamSource("doc_stream")
    
    # Context builder collects join results
    context_builder = LLMContextBuilder()
    
    dim = 4
    
    # Build join pipeline
    pipeline = (
        query_source
        .join(doc_source, create_similarity_join(), dim=dim, parallelism=1)
        .writeSink(context_builder.on_join_result, parallelism=1)
    )
    
    # Add query vectors
    np.random.seed(42)
    for i in range(3):
        query_vec = np.random.randn(dim).astype(np.float32)
        query_vec /= np.linalg.norm(query_vec)  # Normalize
        query_source.addRecord(i, i * 100, query_vec)
        print(f"[Query] Added query uid={i}")
    
    # Add document vectors (some similar to queries)
    for i in range(5):
        if i < 3:
            # Make some docs similar to queries by adding noise
            doc_vec = np.random.randn(dim).astype(np.float32)
            doc_vec /= np.linalg.norm(doc_vec)
        else:
            # Random docs
            doc_vec = np.random.randn(dim).astype(np.float32)
            doc_vec /= np.linalg.norm(doc_vec)
        doc_source.addRecord(100 + i, i * 100 + 50, doc_vec)
        print(f"[Doc] Added doc uid={100 + i}")
    
    # Register streams and execute
    env.addStream(query_source)
    env.addStream(doc_source)
    env.execute()
    
    # Wait for processing
    time.sleep(2.0)
    
    print(f"\n[Result] Built context with {context_builder.processed_count} query-document pairs")
    return context_builder.context_pairs


def run_window_aggregate_pipeline():
    """Run a pipeline with window and aggregate operations."""
    print("\n" + "="*60)
    print("Window Pipeline: source -> window -> aggregate -> sink")
    print("="*60 + "\n")
    
    env = sf.StreamEnvironment()
    source = sf.SimpleStreamSource("event_stream")
    
    aggregated = []
    
    def collect_aggregated(uid: int, ts: int, data: np.ndarray):
        aggregated.append({"uid": uid, "ts": ts, "data": data.copy()})
        print(f"[Aggregated] uid={uid}, ts={ts}, mean_val={np.mean(data):.4f}")
    
    # Build pipeline with window and aggregation
    pipeline = (
        source
        .window(window_size=1000, slide_size=500, window_type=sf.WindowType.Sliding, parallelism=1)
        .aggregate(aggregate_type=sf.AggregateType.Avg, parallelism=1)
        .writeSink(collect_aggregated, parallelism=1)
    )
    
    # Add time-series data
    for i in range(10):
        vec = np.ones(4, dtype=np.float32) * (i + 1)
        source.addRecord(i, i * 200, vec)  # 200ms apart
        print(f"[Source] Added uid={i}, ts={i * 200}, value={i + 1}")
    
    env.addStream(source)
    env.execute()
    
    time.sleep(1.5)
    
    print(f"\n[Result] Aggregated {len(aggregated)} windows")
    return aggregated


def run_full_llm_pipeline():
    """
    Full LLM inference pipeline example:
    
    Query Stream                    Document Stream
         |                               |
      [filter]                       [filter]
         |                               |
       [map]                          [map]
         \\                             /
          \\                           /
           +---- [similarity join] ----+
                        |
                   [context_sink]
                        |
                    LLM Output
    """
    print("\n" + "="*60)
    print("Full LLM Pipeline: RAG-style Query-Document Join")
    print("="*60 + "\n")
    
    env = sf.StreamEnvironment()
    
    # Create sources
    query_source = sf.SimpleStreamSource("user_queries")
    doc_source = sf.SimpleStreamSource("knowledge_base")
    
    dim = 8
    context = LLMContextBuilder()
    
    # Build filtered and normalized query stream
    query_filtered = (
        query_source
        .filter(create_filter_by_norm(0.1), parallelism=1)
        .map(create_normalize_map(), parallelism=1)
    )
    
    # Build filtered and normalized document stream  
    doc_filtered = (
        doc_source
        .filter(create_filter_by_norm(0.1), parallelism=1)
        .map(create_normalize_map(), parallelism=1)
    )
    
    # Join and collect context
    pipeline = (
        query_filtered
        .join(doc_filtered, create_similarity_join(), dim=dim, 
              join_method="bruteforce_lazy", similarity_threshold=0.5, parallelism=1)
        .writeSink(context.on_join_result, parallelism=1)
    )
    
    # Simulate user queries (embeddings)
    np.random.seed(123)
    print("\n--- Adding User Queries ---")
    for i in range(3):
        query = np.random.randn(dim).astype(np.float32)
        query_source.addRecord(i, i * 1000, query)
        print(f"[User] Query {i}: norm={np.linalg.norm(query):.4f}")
    
    # Simulate knowledge base documents
    print("\n--- Adding Knowledge Base Documents ---")
    for i in range(5):
        doc = np.random.randn(dim).astype(np.float32)
        doc_source.addRecord(1000 + i, i * 500, doc)
        print(f"[KB] Document {1000 + i}: norm={np.linalg.norm(doc):.4f}")
    
    # Execute
    print("\n--- Executing Pipeline ---")
    env.addStream(query_source)
    env.addStream(doc_source)
    env.execute()
    
    time.sleep(2.0)
    
    print(f"\n{'='*60}")
    print(f"LLM Context Ready: {context.processed_count} relevant document pairs")
    print(f"{'='*60}")
    
    return context


def main():
    """Run all pipeline examples."""
    print("\n" + "#"*60)
    print("# SageFlow Python API - LLM Pipeline Examples")
    print("#"*60)
    
    # Verify API is available
    print("\n[Setup] Checking SageFlow API...")
    stream_methods = [m for m in dir(sf.Stream) if not m.startswith('_')]
    print(f"[Setup] Stream methods available: {stream_methods}")
    
    # Run examples
    try:
        run_basic_pipeline()
    except Exception as e:
        print(f"[Error] Basic pipeline failed: {e}")
    
    try:
        run_join_pipeline()
    except Exception as e:
        print(f"[Error] Join pipeline failed: {e}")
    
    try:
        run_window_aggregate_pipeline()
    except Exception as e:
        print(f"[Error] Window pipeline failed: {e}")
    
    try:
        run_full_llm_pipeline()
    except Exception as e:
        print(f"[Error] Full LLM pipeline failed: {e}")
    
    print("\n" + "#"*60)
    print("# All examples completed!")
    print("#"*60 + "\n")


if __name__ == "__main__":
    main()
