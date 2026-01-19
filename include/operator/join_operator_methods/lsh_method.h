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
    int max_probes_per_table = 128;      ///< 每个表的最大多探测桶数（含主桶），提高覆盖率
    int max_hamming_radius = 4;          ///< 多探测的最大汉明距离，取平衡的默认值
    int sketch_bits = 8;                 ///< 轻量级预过滤 sketch 位数
    int max_sketch_hamming = 6;          ///< 允许的 sketch 汉明距离阈值（<0 表示禁用预过滤）
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
      int query_slot,
      size_t subtask_index = 0) override;

 private:
  using Hyperplane = std::vector<float>;
    struct Entry {
      std::shared_ptr<const VectorRecord> record;
      uint64_t hash = 0;      ///< table 专属哈希（用于桶命中）
      uint64_t sketch = 0;    ///< 轻量 sketch（用于快速过滤）
      uint16_t left_sig = 0;  ///< 偶位签名（结构分片左）
      uint16_t right_sig = 0; ///< 奇位签名（结构分片右）
    };

  Config config_;
  WindowState* left_state_ = nullptr;
  WindowState* right_state_ = nullptr;
  size_t subtask_index_ = 0;
  std::vector<std::vector<Hyperplane>> tables_;  // tables_[table][hash]
    std::vector<Hyperplane> sketch_planes_;        // 用于生成轻量级 sketch
  
  // 左右签名掩码（兼容保留）
  uint32_t left_mask_ = 0;
  uint32_t right_mask_ = 0;
  int left_bits_ = 0;
  int right_bits_ = 0;

  // 桶结构：tables -> hash -> candidates（左右分开，避免同侧自匹配）
    using BucketMap = std::unordered_map<uint64_t, std::vector<Entry>>;
    std::vector<BucketMap> left_buckets_;
    std::vector<BucketMap> right_buckets_;
    std::vector<std::unique_ptr<std::mutex>> left_bucket_mutexes_;
    std::vector<std::unique_ptr<std::mutex>> right_bucket_mutexes_;
  int64_t window_size_ms_ = 10000;
    bool use_sketch_filter_ = true;        ///< 是否启用 sketch 预过滤

  std::vector<uint64_t> buildProbeKeys(uint64_t base_key,
                                       int max_radius,
                                       size_t max_probes) const;

  void initHyperplanes();
    void initSketchPlanes();
  uint64_t hashVector(const VectorRecord& record, const std::vector<Hyperplane>& planes) const;
    uint64_t sketchVector(const VectorRecord& record) const;
  void splitHash(uint64_t full_hash, uint16_t& left, uint16_t& right) const;
  static std::vector<float> toFloatVector(const VectorRecord& record);
};

}  // namespace sageFlow
