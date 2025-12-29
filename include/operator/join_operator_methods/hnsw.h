#pragma once

#include <deque>
#include <memory>
#include <vector>
#include "operator/join_operator_methods/base_method.h"
#include "function/join_function.h"
#include "concurrency/concurrency_manager.h"

namespace sageFlow {

/**
 * @brief HNSW Join 方法
 *
 * 基于 HNSW (Hierarchical Navigable Small World) 图索引的近似 Join 实现。
 * 利用 HNSW 的层次化导航结构实现高效的近似最近邻搜索。
 *
 * 论文依据:
 * - Malkov, Y.A., Yashunin, D.A. "Efficient and Robust Approximate Nearest
 *   Neighbor Search Using Hierarchical Navigable Small World Graphs"
 *   IEEE TPAMI, 2018. DOI: 10.1109/TPAMI.2018.2889473
 *
 * 核心算法要点:
 * 1. 层次化结构: 多层图结构，上层稀疏用于快速跳转，下层稠密用于精确搜索
 * 2. Navigable Small World: 每层维护 small world 属性，保证 O(log N) 搜索复杂度
 * 3. 增量插入: 支持高效增量插入，不需要完全重建
 * 4. 可调参数: M (最大邻居数)、efConstruction (构建质量)、efSearch (搜索质量)
 */
class HNSWJoinMethod final : public BaseMethod {
 public:
  /**
   * @brief HNSW 方法配置
   */
  struct Config {
    int m;                           ///< 每层最大邻居数（除第0层外）
    int ef_construction;             ///< 构建时候选集大小（影响图质量）
    int ef_search;                   ///< 搜索时候选集大小（影响召回率）
    bool use_existing_index;         ///< 是否复用已有 HNSW 索引
    
    Config() : m(16), ef_construction(200), ef_search(100), use_existing_index(true) {}
  };

  /**
   * @brief HNSW 索引统计信息
   */
  struct IndexStats {
    size_t num_elements = 0;  ///< 索引中的元素数量
    size_t num_layers = 0;    ///< 最大层数
    size_t memory_usage = 0;  ///< 估计内存使用量（字节）
  };

  /**
   * @brief 构造函数
   * @param left_index_id 左侧索引 ID
   * @param right_index_id 右侧索引 ID
   * @param join_similarity_threshold 相似度阈值
   * @param concurrency_manager 并发管理器
   * @param config HNSW 配置参数
   */
  HNSWJoinMethod(int left_index_id,
                 int right_index_id,
                 double join_similarity_threshold,
                 const std::shared_ptr<ConcurrencyManager>& concurrency_manager,
                 const Config& config = Config());

  ~HNSWJoinMethod() override = default;

  /**
   * @brief 获取方法名称
   * @return 方法名称字符串
   */
  std::string getName() const { return "HNSW"; }

  /**
   * @brief Eager 模式执行（单查询）
   *
   * 对单个查询向量执行范围搜索，返回所有满足相似度阈值的候选记录。
   *
   * @param query_record 查询向量记录
   * @param query_slot 查询槽位（0=左流，1=右流）
   * @param subtask_index 当前执行的 subtask 索引
   * @return 满足阈值的候选记录列表
   */
  std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
      const VectorRecord& query_record, int query_slot,
      size_t subtask_index = 0) override;

  /**
   * @brief 设置搜索扩展因子
   *
   * 动态调整搜索时的候选集大小。较大的 ef_search 会提高召回率但降低速度。
   *
   * @param ef_search 新的搜索扩展因子
   */
  void setEfSearch(int ef_search);

  /**
   * @brief 获取当前配置
   * @return 配置的常量引用
   */
  const Config& getConfig() const { return config_; }

  /**
   * @brief 获取索引统计信息
   * @return 索引统计信息
   */
  IndexStats getStats() const;

 private:
  /**
   * @brief 获取对侧索引 ID
   * @param slot 当前槽位
   * @return 对侧索引 ID
   */
  inline int otherIndexId(int slot) const {
    return (slot == 0) ? right_index_id_ : left_index_id_;
  }

  /**
   * @brief 使用 k-NN 模拟范围搜索
   *
   * HNSW 原生不支持范围搜索，通过 k-NN 查询后过滤实现。
   * 策略:
   * 1. 使用 ef_search 作为初始 k 值
   * 2. 执行 k-NN 查询
   * 3. 过滤满足相似度阈值的结果
   *
   * @param query_record 查询向量
   * @param index_id 目标索引 ID
   * @return 满足阈值的候选记录
   */
  std::vector<std::shared_ptr<const VectorRecord>> rangeSearchViaKNN(
      const VectorRecord& query_record, int index_id);

  int left_index_id_ = -1;
  int right_index_id_ = -1;
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
  Config config_;
};

}  // namespace sageFlow
