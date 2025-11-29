#pragma once

#include <algorithm>
#include <deque>
#include <memory>
#include <shared_mutex>
#include <vector>

#include "compute_engine/pca.h"
#include "index/index.h"

namespace sageFlow {

/**
 * @brief HDR-Tree (High-Dimensional R-Tree) 索引
 *
 * 基于 PCA 降维和 R-Tree 的高维向量索引，参考论文：
 * - "Efficient kNN Join over Dynamic High-Dimensional Data" (ADC 2022)
 * - "Efficient kNN Join over Dynamic High-Dimensional Data" (WWW Journal 2023)
 *
 * 核心思想：
 * 1. 使用 PCA 将高维向量投影到低维空间
 * 2. 在低维空间使用 R-Tree 进行快速剪枝
 * 3. 对候选向量在原始空间进行精确验证
 *
 * 距离估计公式：
 * d'(v', q') <= d(v, q) <= α * d'(v', q')
 * 其中 α 由奇异值分布决定
 */
class HDRTree final : public Index {
 public:
  /**
   * @brief HDR-Tree 配置参数
   */
  struct Config {
    int projected_dim;             ///< 降维后维度
    int rtree_min_entries;          ///< R-Tree 节点最小条目数
    int rtree_max_entries;         ///< R-Tree 节点最大条目数
    int pca_sample_size;        ///< PCA 训练样本数
    float distance_bound_ratio;  ///< 距离上界比例因子

    /// 默认构造函数
    Config() : projected_dim(16), rtree_min_entries(4), rtree_max_entries(16),
               pca_sample_size(10000), distance_bound_ratio(1.2f) {}
  };

  /**
   * @brief R-Tree 节点
   */
  struct RTreeNode {
    std::vector<float> mbr_low;                      ///< 最小边界矩形下界
    std::vector<float> mbr_high;                     ///< 最小边界矩形上界
    std::vector<uint64_t> entries;                   ///< 叶子节点的数据条目
    std::vector<std::unique_ptr<RTreeNode>> children;  ///< 子节点
    bool is_leaf = true;                             ///< 是否为叶子节点

    /**
     * @brief 构造空节点
     * @param dim 维度
     */
    explicit RTreeNode(int dim);

    /**
     * @brief 检查 MBR 是否与查询范围相交
     * @param query 查询点
     * @param threshold 距离阈值
     * @return 是否相交
     */
    [[nodiscard]] auto intersects(const std::vector<float>& query, float threshold) const -> bool;

    /**
     * @brief 更新 MBR 以包含新点
     * @param point 新点
     */
    void expandMBR(const std::vector<float>& point);
  };

  /**
   * @brief 构造函数
   * @param dimension 原始向量维度
   * @param config 配置参数
   */
  explicit HDRTree(int dimension, const Config& config = Config{});

  /**
   * @brief 析构函数
   */
  ~HDRTree() override = default;

  // 禁止拷贝
  HDRTree(const HDRTree&) = delete;
  auto operator=(const HDRTree&) -> HDRTree& = delete;

  // 允许移动
  HDRTree(HDRTree&&) noexcept = default;
  auto operator=(HDRTree&&) noexcept -> HDRTree& = default;

  /**
   * @brief 使用样本数据训练 PCA
   * @param samples 样本向量
   */
  void trainPCA(const std::vector<std::vector<float>>& samples);

  /**
   * @brief 检查 PCA 是否已训练
   * @return 是否已训练
   */
  [[nodiscard]] auto isPCATrained() const -> bool { return pca_ && pca_->isFitted(); }

  /**
   * @brief 插入向量
   * @param uid 向量唯一ID
   * @return 是否成功
   */
  auto insert(uint64_t uid) -> bool override;

  /**
   * @brief 删除向量
   * @param uid 向量唯一ID
   * @return 是否成功
   */
  auto erase(uint64_t uid) -> bool override;

  /**
   * @brief k-NN 查询
   * @param record 查询向量
   * @param k 返回数量
   * @return 结果向量的 UID 列表
   */
  auto query(const VectorRecord& record, int k) -> std::vector<uint64_t> override;

  /**
   * @brief 基于阈值的范围查询（用于 Join）
   * @param record 查询向量
   * @param threshold 相似度阈值
   * @return 满足阈值的向量 UID 列表
   */
  auto query_for_join(const VectorRecord& record, double threshold) -> std::vector<uint64_t> override;

  /**
   * @brief 获取索引大小
   * @return 索引中向量数量
   */
  [[nodiscard]] auto size() const -> size_t { return uid_to_projected_.size(); }

  /**
   * @brief 获取配置
   * @return 配置引用
   */
  [[nodiscard]] auto getConfig() const -> const Config& { return config_; }

  /**
   * @brief 获取 PCA 投影器
   * @return PCA 指针
   */
  [[nodiscard]] auto getPCA() const -> const PCA* { return pca_.get(); }

 private:
  Config config_;                          ///< 配置参数
  std::unique_ptr<PCA> pca_;               ///< PCA 投影器
  std::unique_ptr<RTreeNode> rtree_root_;  ///< R-Tree 根节点

  /// UID 到投影向量的映射
  std::unordered_map<uint64_t, std::vector<float>> uid_to_projected_;

  /// 采样缓冲区（用于延迟训练 PCA）
  std::vector<std::vector<float>> sample_buffer_;
  bool pca_training_done_ = false;

  /// 并发控制
  mutable std::shared_mutex mutex_;

  /**
   * @brief 将向量投影到低维空间
   * @param data 原始向量数据
   * @return 投影后的向量
   */
  [[nodiscard]] auto projectVector(const VectorData& data) const -> std::vector<float>;

  /**
   * @brief 在 R-Tree 中搜索候选
   * @param projected_query 投影后的查询向量
   * @param threshold 距离阈值
   * @return 候选 UID 列表
   */
  [[nodiscard]] auto searchRTree(const std::vector<float>& projected_query, float threshold) const
      -> std::vector<uint64_t>;

  /**
   * @brief 递归搜索 R-Tree 节点
   * @param node 当前节点
   * @param query 查询向量
   * @param threshold 距离阈值
   * @param candidates 输出候选列表
   */
  void searchRTreeNode(const RTreeNode* node, const std::vector<float>& query, float threshold,
                       std::vector<uint64_t>& candidates) const;

  /**
   * @brief 验证候选并计算精确距离
   * @param query 查询向量记录
   * @param candidates 候选 UID 列表
   * @param threshold 相似度阈值
   * @return 满足阈值的 UID 列表
   */
  [[nodiscard]] auto verifyCandidates(const VectorRecord& query,
                                       const std::vector<uint64_t>& candidates,
                                       double threshold) const -> std::vector<uint64_t>;

  /**
   * @brief 将向量插入 R-Tree
   * @param uid 向量 UID
   * @param projected 投影后的向量
   */
  void insertToRTree(uint64_t uid, const std::vector<float>& projected);

  /**
   * @brief 计算两个投影向量的欧氏距离
   * @param v1 向量1
   * @param v2 向量2
   * @return 欧氏距离
   */
  [[nodiscard]] static auto euclideanDistance(const std::vector<float>& v1,
                                               const std::vector<float>& v2) -> float;

  /**
   * @brief 估计原始空间距离上界
   * @param projected_dist 投影空间距离
   * @return 估计的原始空间距离上界
   */
  [[nodiscard]] auto estimateDistanceUpperBound(float projected_dist) const -> float;

  /**
   * @brief 从 VectorData 提取浮点数组
   * @param data 向量数据
   * @return 浮点数向量
   */
  [[nodiscard]] static auto extractFloatVector(const VectorData& data) -> std::vector<float>;

  /**
   * @brief 尝试自动训练 PCA（当样本缓冲区足够时）
   */
  void tryAutoTrainPCA();
};

}  // namespace sageFlow
