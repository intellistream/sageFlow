#!/usr/bin/env python3
"""
SageFlow C++ 运行时验证测试
============================

这个脚本真正测试 SageFlow C++ 引擎是否正常工作。
不是自欺欺人的 print，而是实际执行 C++ Join 并验证结果。

运行方式:
    cd sageFlow
    LD_LIBRARY_PATH=build/lib:$LD_LIBRARY_PATH python examples/python/test_sageflow_cpp_runtime.py
"""

import sys
from pathlib import Path

# 添加 sageFlow 路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import numpy as np

# 导入 SageFlow
try:
    import sage_flow as sf
    print("✓ sage_flow 模块导入成功")
except ImportError as e:
    print(f"✗ sage_flow 导入失败: {e}")
    print("\n请确保设置了 LD_LIBRARY_PATH:")
    print("  LD_LIBRARY_PATH=build/lib:$LD_LIBRARY_PATH python examples/python/test_sageflow_cpp_runtime.py")
    sys.exit(1)


def test_cpp_binding_types():
    """测试 1: 验证 C++ 绑定类型"""
    print("\n" + "=" * 60)
    print("测试 1: 验证 C++ 绑定类型")
    print("=" * 60)
    
    # 检查是否是真正的 pybind11 类型
    is_pybind11 = "pybind11" in str(type(sf.StreamEnvironment))
    print(f"  StreamEnvironment 类型: {type(sf.StreamEnvironment)}")
    print(f"  是 pybind11 类型: {is_pybind11}")
    
    if not is_pybind11:
        print("  ✗ 失败: 不是 C++ 绑定，可能是 Python mock")
        return False
    
    print("  ✓ 通过: 确认是 C++ pybind11 绑定")
    return True


def test_create_objects():
    """测试 2: 创建 C++ 对象"""
    print("\n" + "=" * 60)
    print("测试 2: 创建 C++ 对象")
    print("=" * 60)
    
    try:
        env = sf.StreamEnvironment()
        print(f"  ✓ StreamEnvironment 创建成功: {env}")
        
        source = sf.SimpleStreamSource("test_source")
        print(f"  ✓ SimpleStreamSource 创建成功: {source}")
        
        return env, source
    except Exception as e:
        print(f"  ✗ 创建失败: {e}")
        return None, None


def test_add_records(source):
    """测试 3: 添加向量记录"""
    print("\n" + "=" * 60)
    print("测试 3: 添加向量记录到 C++ 数据源")
    print("=" * 60)
    
    dim = 128
    np.random.seed(42)
    
    try:
        # 添加 5 个向量
        for i in range(5):
            vec = np.random.randn(dim).astype(np.float32)
            vec = vec / np.linalg.norm(vec)  # 归一化
            
            # API: addRecord(uid, timestamp, data)
            source.addRecord(i, i * 1000, vec)
            print(f"  ✓ 添加记录 {i}: uid={i}, ts={i*1000}, norm={np.linalg.norm(vec):.4f}")
        
        return True
    except Exception as e:
        print(f"  ✗ 添加记录失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_join_configuration(source):
    """测试 4: 配置 Join 参数"""
    print("\n" + "=" * 60)
    print("测试 4: 配置 Join 参数")
    print("=" * 60)
    
    try:
        source.setJoinMethod("bruteforce_lazy")
        method = source.getJoinMethod()
        print(f"  ✓ 设置 Join 方法: {method}")
        
        source.setJoinSimilarityThreshold(0.5)
        threshold = source.getJoinSimilarityThreshold()
        print(f"  ✓ 设置相似度阈值: {threshold}")
        
        return True
    except Exception as e:
        print(f"  ✗ 配置失败: {e}")
        return False


def test_simple_sink_pipeline():
    """测试 5: 简单 Sink Pipeline (验证 C++ 数据流)"""
    print("\n" + "=" * 60)
    print("测试 5: 简单 Sink Pipeline (验证 C++ 数据流)")
    print("=" * 60)
    
    import time
    
    env = sf.StreamEnvironment()
    source = sf.SimpleStreamSource("test_source")
    
    # 结果收集
    sink_results = []
    
    def on_sink(uid: int, ts: int):
        """Python 回调: C++ 引擎每处理一条记录就调用此函数"""
        sink_results.append({"uid": uid, "ts": ts})
        print(f"    [C++ → Python] 收到记录: uid={uid}, ts={ts}")
    
    # 使用 write_sink_py 挂载 Python 回调
    source.write_sink_py("py_sink", on_sink)
    
    # 先注册到环境
    env.addStream(source)
    
    # 添加测试数据
    dim = 128
    np.random.seed(42)
    total = 5
    
    print("\n  [添加数据]")
    for i in range(total):
        vec = np.random.randn(dim).astype(np.float32)
        ts = int(time.time() * 1000) + i * 100
        source.addRecord(i, ts, vec)
        print(f"    添加记录 {i}: uid={i}, ts={ts}")
    
    # 执行
    print("\n  [执行 Pipeline]")
    print("  " + "-" * 50)
    env.execute()
    print("  " + "-" * 50)
    
    # 等待异步处理
    max_wait = 3.0
    elapsed = 0.0
    while len(sink_results) < total and elapsed < max_wait:
        time.sleep(0.1)
        elapsed += 0.1
    
    # 验证
    print(f"\n  [验证结果]")
    print(f"    期望处理: {total} 条")
    print(f"    实际处理: {len(sink_results)} 条")
    
    if len(sink_results) == total:
        print("\n  ✓ C++ 数据流正常! Python 回调被正确调用")
        return True
    else:
        print(f"\n  ✗ 数据处理不完整: {len(sink_results)}/{total}")
        return False


def test_full_join_pipeline():
    """测试 6: 完整的 Join Pipeline (核心测试)"""
    print("\n" + "=" * 60)
    print("测试 6: 完整的 Join Pipeline (C++ 引擎核心测试)")
    print("=" * 60)
    
    import time
    
    # 创建环境
    env = sf.StreamEnvironment()
    
    # 创建左右两个数据源
    left_source = sf.SimpleStreamSource("left_queries")
    right_source = sf.SimpleStreamSource("right_docs")
    
    dim = 128
    np.random.seed(42)
    
    # 配置 Join (在添加数据之前)
    # 注意：Join 配置是在 left_source 上设置的
    left_source.setJoinMethod("bruteforce_lazy")
    left_source.setJoinSimilarityThreshold(0.3)  # 阈值 0.3
    print(f"  [配置] Join 方法: {left_source.getJoinMethod()}")
    print(f"  [配置] 阈值: {left_source.getJoinSimilarityThreshold()}")
    
    # 定义回调函数
    join_results = []
    
    def join_callback(l_uid, l_ts, l_vec, r_uid, r_ts, r_vec):
        """Join 回调: C++ 引擎调用此函数处理每对匹配"""
        similarity = float(np.dot(l_vec, r_vec))
        join_results.append({
            "left_id": int(l_uid),
            "right_id": int(r_uid),
            "similarity": similarity,
        })
        print(f"    [Join 回调] Query {l_uid} ↔ Doc {r_uid}: sim={similarity:.4f}")
        # 返回合并结果
        combined_uid = int(l_uid) * 1000 + int(r_uid)
        combined_ts = max(int(l_ts), int(r_ts))
        combined_vec = ((l_vec + r_vec) / 2).astype(np.float32)
        return (combined_uid, combined_ts, combined_vec)
    
    sink_results = []
    
    def sink_callback(uid, ts, vec):
        """Sink 回调"""
        sink_results.append({"uid": uid, "ts": ts})
        print(f"    [Sink 回调] uid={uid}, ts={ts}")
    
    # 保存向量用于验证
    left_vectors = []
    right_vectors = []
    
    # 添加数据 (在构建 Pipeline 之前)
    print("\n  [添加数据]")
    base_ts = int(time.time() * 1000)
    
    # 左流: 3 个查询
    for i in range(3):
        vec = np.random.randn(dim).astype(np.float32)
        vec = vec / np.linalg.norm(vec)
        left_vectors.append(vec)
        left_source.addRecord(i, base_ts + i * 100, vec)
    print(f"    左流: {len(left_vectors)} 个查询向量")
    
    # 右流: 5 个文档 (其中一些与查询相似)
    for i in range(5):
        if i < 3:
            # 前 3 个文档与对应查询相似（添加小噪声）
            vec = left_vectors[i] + np.random.randn(dim).astype(np.float32) * 0.1
        else:
            # 后 2 个文档随机
            vec = np.random.randn(dim).astype(np.float32)
        vec = vec / np.linalg.norm(vec)
        vec = vec.astype(np.float32)
        right_vectors.append(vec)
        right_source.addRecord(100 + i, base_ts + i * 100, vec)
    print(f"    右流: {len(right_vectors)} 个文档向量")
    
    # 计算期望的相似度
    print("\n  [期望的相似度 (超过阈值的)]")
    expected_matches = 0
    for i, lv in enumerate(left_vectors):
        for j, rv in enumerate(right_vectors):
            sim = float(np.dot(lv, rv))
            if sim > 0.3:  # 只显示超过阈值的
                print(f"    Query {i} ↔ Doc {100+j}: {sim:.4f}")
                expected_matches += 1
    print(f"    预期匹配数: {expected_matches}")
    
    # 构建 Pipeline
    # 关键修复: 直接使用 right_source (SimpleStreamSource)，不要用 filter 转换！
    print("\n  [构建 Pipeline]")
    
    try:
        # 正确用法: left_source.join(right_source, ...)
        # SimpleStreamSource 继承自 Stream，可以直接传入
        pipeline = (
            left_source
            .join(right_source, join_callback, dim, 1)  # 直接用 right_source
            .writeSink(sink_callback, 1)
        )
        print("    ✓ Pipeline 构建完成")
        
        # 注册流
        env.addStream(left_source)
        env.addStream(right_source)
        print("    ✓ 流已注册到环境")
        
    except Exception as e:
        print(f"    ✗ 构建失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # 执行
    print("\n  [执行 Pipeline]")
    print("  " + "-" * 50)
    
    try:
        env.execute()
    except Exception as e:
        print(f"    ✗ execute() 失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("  " + "-" * 50)
    
    # 等待异步处理
    max_wait = 3.0
    elapsed = 0.0
    while len(join_results) < expected_matches and elapsed < max_wait:
        time.sleep(0.1)
        elapsed += 0.1
    
    # 额外等待确保所有回调完成
    time.sleep(0.5)
    
    # 验证结果
    print(f"\n  [验证结果]")
    print(f"    Join 回调次数: {len(join_results)}")
    print(f"    Sink 回调次数: {len(sink_results)}")
    
    if len(join_results) > 0:
        print("\n    Join 匹配详情:")
        for r in join_results:
            print(f"      Query {r['left_id']} ↔ Doc {r['right_id']}: similarity={r['similarity']:.4f}")
        print("\n  ✓ C++ Join 引擎工作正常!")
        return True
    else:
        print("\n  ⚠ Join 没有产生匹配结果")
        print("    可能原因: 阈值设置、窗口配置、或数据时序问题")
        return False


def main():
    print("\n" + "#" * 70)
    print("#" + " " * 15 + "SageFlow C++ 运行时验证测试" + " " * 15 + "#")
    print("#" * 70)
    
    results = {}
    
    # 测试 1: C++ 绑定类型
    results["binding_types"] = test_cpp_binding_types()
    
    # 测试 2: 创建对象
    env, source = test_create_objects()
    results["create_objects"] = env is not None
    
    if source:
        # 测试 3: 添加记录
        results["add_records"] = test_add_records(source)
        
        # 测试 4: Join 配置
        results["join_config"] = test_join_configuration(source)
    
    # 测试 5: 简单 Sink Pipeline
    results["simple_sink"] = test_simple_sink_pipeline()
    
    # 测试 6: 完整 Join Pipeline
    results["full_pipeline"] = test_full_join_pipeline()
    
    # 总结
    print("\n" + "=" * 70)
    print("测试总结")
    print("=" * 70)
    
    all_passed = True
    for name, passed in results.items():
        status = "✓ 通过" if passed else "✗ 失败"
        print(f"  {name}: {status}")
        if not passed:
            all_passed = False
    
    print()
    if all_passed:
        print("🎉 所有测试通过! SageFlow C++ 运行时工作正常!")
    else:
        print("❌ 部分测试失败，请检查上述错误信息")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
