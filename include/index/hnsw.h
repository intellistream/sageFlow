#include <queue>
#include <random>

#include "index/index.h"

namespace candy {
class HNSW final : public Index {
 public:
  // HNSW() : HNSW(20, 100, 40) {}
  explicit HNSW(int m = 20, int ef_construction = 100, int ef_search = 40);

  ~HNSW() override = default;

  auto insert(uint64_t uid) -> bool override;
  auto erase(uint64_t uid) -> bool override;
  auto query(std::unique_ptr<VectorRecord>& record, int k) -> std::vector<uint64_t> override;
  auto select_neighbors_heuristic(const VectorRecord& q, const std::vector<uint64_t>& c, int m, int lc,
                                  bool extend_candidates, bool keep_pruned_connections) const -> std::vector<uint64_t>;
  inline auto select_neighbors_basic(const VectorRecord& q, const std::vector<uint64_t>& C, int M,
                                     int lc) const -> std::vector<uint64_t>;
  auto query_for_join(std::unique_ptr<VectorRecord> &record,
                            double join_similarity_threshold) -> std::vector<uint64_t> override {
    // NOT IMPLEMENTED;
    return {};
  }
 private:
  struct Neighbor {
    uint64_t id_;  // uid（即 storage_engine 中的 id）
    double dist_;   // 与查询向量的 L2 距离

    auto operator<(Neighbor const& other) const -> bool { return dist_ < other.dist_; } 
    auto operator>(Neighbor const& other) const -> bool { return dist_ > other.dist_; }  
  };

  struct Node {
    uint64_t id_;                               // uid
    int level_;                                 // 节点的最高层级
    std::vector<std::vector<uint64_t>> links_;  // links[l] 为该节点在第 l 层的邻居 uid 列表
  };

  // ---------- 参数 ----------
  int m_;                // 每层最大邻居数（除顶层）
  int ef_construction_;  // 构建时候选集大小
  int ef_search_;        // 查询时候选集大小

  // ---------- 状态 ----------
  int max_level_ = -1;  // 当前最高层级
  uint64_t entry_point_ = std::numeric_limits<uint64_t>::max();
  std::unordered_map<uint64_t, Node> nodes_;  // uid -> Node
  std::mt19937 rng_{std::random_device{}()};

  // ---------- 内部工具 ----------
  auto l2_distance(const VectorRecord& a, const VectorRecord& b) const -> double;
  void search_layer(const VectorRecord& q, std::priority_queue<Neighbor>& top_candidates, int layer, int ef) const;

  auto random_level() -> int;
};
}  // namespace candy
