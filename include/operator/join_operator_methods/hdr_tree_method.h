#pragma once

#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "concurrency/concurrency_manager.h"
#include "function/join_function.h"
#include "index/hdr_tree.h"
#include "operator/join_operator_methods/base_method.h"

namespace sageFlow {

/**
 * @brief HDR-Tree Join 方法
 *
 * 基于 HDR-Tree 索引的向量相似度 Join 方法。
 * 使用 PCA 降维进行快速候选过滤，然后在原始空间验证。
 *
 * 参考论文：
 * - "Efficient kNN Join over Dynamic High-Dimensional Data" (ADC 2022, WWW 2023)
 *
 * 推荐配置：
 * - partition_strategy: key_hash
 * - window_state_type: partitioned
 * - index_strategy: hdr_tree
 */
class HDRTreeMethod final : public BaseMethod {
 public:
  /**
   * @brief 配置参数
   */
  struct Config {
    double similarity_threshold;  ///< 相似度阈值
    int projected_dim;             ///< PCA 降维目标维度
    int pca_sample_size;        ///< PCA 训练样本数
    float distance_bound_ratio;  ///< 距离上界比例因子
    
    Config() : similarity_threshold(0.8), projected_dim(16),
               pca_sample_size(10000), distance_bound_ratio(1.2f) {}
  };

  /**
   * @brief 构造函数
   * @param left_index_id 左侧索引 ID
   * @param right_index_id 右侧索引 ID
   * @param similarity_threshold 相似度阈值
   * @param concurrency_manager 并发管理器
   * @param config 配置参数
   */
  HDRTreeMethod(int left_index_id, int right_index_id, double similarity_threshold,
                const std::shared_ptr<ConcurrencyManager>& concurrency_manager,
                const Config& config = Config());

  /**
   * @brief 析构函数
   */
  ~HDRTreeMethod() override = default;

  /**
   * @brief 获取方法名称
   * @return 方法名称
   */
  [[nodiscard]] auto getName() const -> std::string { return "HDR-Tree"; }

  /**
   * @brief Eager 执行模式（单查询）
   * @param query_record 查询记录
   * @param query_slot 查询来源槽位（0=左，1=右）
   * @return 候选结果列表
   */
  auto ExecuteEager(const VectorRecord& query_record, int query_slot)
      -> std::vector<std::unique_ptr<VectorRecord>> override;

  /**
   * @brief 获取配置
   * @return 配置引用
   */
  [[nodiscard]] auto getConfig() const -> const Config& { return config_; }

 private:
  /**
   * @brief 获取对侧索引 ID
   * @param slot 当前槽位
   * @return 对侧索引 ID
   */
  [[nodiscard]] auto otherIndexId(int slot) const -> int {
    return (slot == 0) ? right_index_id_ : left_index_id_;
  }

  Config config_;                                      ///< 配置参数
  int left_index_id_ = -1;                             ///< 左侧索引 ID
  int right_index_id_ = -1;                            ///< 右侧索引 ID
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;  ///< 并发管理器
};

}  // namespace sageFlow
