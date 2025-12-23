#pragma once

#include <random>
#include <vector>
#include <memory>
#include <cstdint>
#include <unordered_map>
#include <mutex>

#include "operator/join_operator_methods/base_method.h"
#include "state/window_state.h"
#include "compute_engine/simd_distance.h"
#include "execution/runtime_context.h"

namespace sageFlow {

/**
 * @brief 基于超平面的简易 LSH Join 方法（Eager 模式）
 *
 * 设计目标：
 * - 不依赖外部索引，直接使用 WindowState 快照构建桶并筛选候选。
 * - 先实现最小可用版本，后续可替换为更高性能的数据结构。
 */
class LSHMethod final : public BaseMethod {
 public:
  struct Config {
    double similarity_threshold = 0.8;   ///< 相似度阈值
    int num_tables = 4;                  ///< LSH 表数量
    int num_hashes = 8;                  ///< 每表超平面数
    int dimension = 128;                 ///< 向量维度
    uint32_t seed = 42;                  ///< 随机种子
    int64_t window_size_ms = 10000;      ///< 窗口大小（用于过期过滤）
    int max_probes_per_table = 4;        ///< 每个表的最大多探测桶数（含主桶）
    int max_hamming_radius = 2;          ///< 多探测的最大汉明距离
    size_t min_candidates = 0;           ///< 若>0且候选低于阈值则扩展/兜底
    bool fallback_on_sparse = false;     ///< 默认关闭兜底，纯 LSH 路径
  };

  explicit LSHMethod(const Config& config);

  /**
   * @brief 初始化方法，记录窗口指针与任务上下文
   */
  void open(const RuntimeContext& context,
            WindowState* left_state,
            WindowState* right_state);

  /**
   * @brief 设置窗口大小（由 JoinOperator 注入，以便进行过期过滤）
   */
  void setWindowSize(int64_t window_size_ms) { window_size_ms_ = window_size_ms; }

  /**
   * @brief 插入新记录到桶结构（由 JoinOperator 在窗口更新时调用）
   */
  void onRecordAdded(const VectorRecord& record, int slot);

  std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
      const VectorRecord& query_record,
      int query_slot) override;

 private:
  using Hyperplane = std::vector<float>;

  Config config_;
  WindowState* left_state_ = nullptr;
  WindowState* right_state_ = nullptr;
  size_t subtask_index_ = 0;
  std::vector<std::vector<Hyperplane>> tables_;  // tables_[table][hash]

  // 桶结构：tables -> hash -> candidates
  using BucketMap = std::unordered_map<uint64_t, std::vector<std::shared_ptr<const VectorRecord>>>;
  std::vector<BucketMap> buckets_;
  std::mutex buckets_mutex_;
  int64_t window_size_ms_ = 10000;

  std::vector<uint64_t> buildProbeKeys(uint64_t base_key) const;

  void initHyperplanes();
  uint64_t hashVector(const VectorRecord& record, const std::vector<Hyperplane>& planes) const;
  static std::vector<float> toFloatVector(const VectorRecord& record);
};

}  // namespace sageFlow
