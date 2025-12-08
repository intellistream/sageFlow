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
 * 基于 PCA 降维和 R-Tree 的高维向量索引。
 * 实现了标准的 R-Tree 插入、分裂（Quadratic Split）和查询逻辑。
 */
class HDRTree final : public Index {
 public:
  /**
   * @brief HDR-Tree 配置参数
   */
  struct Config {
    int projected_dim;             ///< 降维后维度
    int rtree_min_entries;         ///< R-Tree 节点最小条目数
    int rtree_max_entries;         ///< R-Tree 节点最大条目数
    int pca_sample_size;           ///< PCA 训练样本数
    float distance_bound_ratio;    ///< 距离上界比例因子

    Config() : projected_dim(16), rtree_min_entries(4), rtree_max_entries(16),
               pca_sample_size(10000), distance_bound_ratio(1.2f) {}
  };

  /**
   * @brief R-Tree 节点
   */
  struct RTreeNode {
    std::vector<float> mbr_low;                      ///< MBR 下界
    std::vector<float> mbr_high;                     ///< MBR 上界
    std::vector<uint64_t> entries;                   ///< 叶子节点数据 (UID)
    std::vector<std::unique_ptr<RTreeNode>> children;  ///< 内部节点子树
    bool is_leaf = true;                             ///< 是否为叶子节点

    explicit RTreeNode(int dim);

    // 计算 MBR 面积（体积）
    [[nodiscard]] auto area() const -> float;

    // 计算包含新点所需的面积增量
    [[nodiscard]] auto enlargement(const std::vector<float>& point) const -> float;

    // 计算包含另一个 MBR 所需的面积增量
    [[nodiscard]] auto enlargement(const RTreeNode& other) const -> float;

    // 检查相交
    [[nodiscard]] auto intersects(const std::vector<float>& query, float threshold) const -> bool;

    // 扩展 MBR 以包含点
    void expandMBR(const std::vector<float>& point);

    // 扩展 MBR 以包含另一个节点
    void expandMBR(const RTreeNode& other);
  };

  explicit HDRTree(int dimension, const Config& config = Config{});
  ~HDRTree() override = default;

  HDRTree(const HDRTree&) = delete;
  auto operator=(const HDRTree&) -> HDRTree& = delete;
  HDRTree(HDRTree&&) noexcept = default;
  auto operator=(HDRTree&&) noexcept -> HDRTree& = default;

  void trainPCA(const std::vector<std::vector<float>>& samples);
  [[nodiscard]] auto isPCATrained() const -> bool { return pca_ && pca_->isFitted(); }

  auto insert(uint64_t uid) -> bool override;
  auto insert(uint64_t uid, const std::vector<float>& projected) -> bool;
  auto erase(uint64_t uid) -> bool override;
  auto query(const VectorRecord& record, int k) -> std::vector<uint64_t> override;
  auto query_for_join(const VectorRecord& record, double threshold) -> std::vector<uint64_t> override;

  [[nodiscard]] auto size() const -> size_t { return uid_to_projected_.size(); }
  [[nodiscard]] auto getConfig() const -> const Config& { return config_; }
  [[nodiscard]] auto getPCA() const -> const PCA* { return pca_.get(); }

 private:
  Config config_;
  std::unique_ptr<PCA> pca_;
  std::unique_ptr<RTreeNode> rtree_root_;
  std::unordered_map<uint64_t, std::vector<float>> uid_to_projected_;
  std::vector<std::vector<float>> sample_buffer_;
  bool pca_training_done_ = false;
  mutable std::shared_mutex mutex_;

  // 核心 R-Tree 操作
  void insertToRTree(uint64_t uid, const std::vector<float>& projected);
  
  // 递归插入，返回分裂产生的新节点（如果有）
  std::unique_ptr<RTreeNode> insertRecursive(RTreeNode* node, uint64_t uid, const std::vector<float>& point);
  
  // 节点分裂逻辑 (Quadratic Split)
  std::unique_ptr<RTreeNode> splitLeafNode(RTreeNode* node);
  std::unique_ptr<RTreeNode> splitInternalNode(RTreeNode* node);
  
  // 选择最佳插入子节点 (ChooseLeaf)
  auto chooseLeaf(RTreeNode* node, const std::vector<float>& point) -> RTreeNode*;

  // 辅助函数
  [[nodiscard]] auto projectVector(const VectorData& data) const -> std::vector<float>;
  [[nodiscard]] auto searchRTree(const std::vector<float>& projected_query, float threshold) const -> std::vector<uint64_t>;
  void searchRTreeNode(const RTreeNode* node, const std::vector<float>& query, float threshold, std::vector<uint64_t>& candidates) const;
  [[nodiscard]] auto verifyCandidates(const VectorRecord& query, const std::vector<uint64_t>& candidates, double threshold) const -> std::vector<uint64_t>;
  [[nodiscard]] static auto euclideanDistance(const std::vector<float>& v1, const std::vector<float>& v2) -> float;
  [[nodiscard]] auto estimateDistanceUpperBound(float projected_dist) const -> float;
  [[nodiscard]] static auto extractFloatVector(const VectorData& data) -> std::vector<float>;
  void tryAutoTrainPCA();
};

}  // namespace sageFlow
