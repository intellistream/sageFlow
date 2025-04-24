#include "index/index.h"

namespace candy {
    class HNSW final : public Index {
        public:
         explicit HNSW(int m = 16, int efConstruction = 200, int efSearch = 50);
         ~HNSW() override = default;
       
         bool insert(uint64_t uid) override;
         bool erase(uint64_t uid) override;
         std::vector<int32_t> query(std::unique_ptr<VectorRecord>& record, int k) override;
       
        private:
         struct Neighbor {
           uint64_t id;  // uid（即 storage_engine 中的 id）
           float dist;   // 与查询向量的 L2 距离
       
           bool operator<(Neighbor const& other) const { return dist > other.dist; }  // 小顶堆
         };
       
         struct Node {
           uint64_t id;                               // uid
           int level;                                 // 节点的最高层级
           std::vector<std::vector<uint64_t>> links;  // links[l] 为该节点在第 l 层的邻居 uid 列表
         };
       
         // ---------- 参数 ----------
         int M_;                // 每层最大邻居数（除顶层）
         int ef_construction_;  // 构建时候选集大小
         int ef_search_;        // 查询时候选集大小
       
         // ---------- 状态 ----------
         int max_level_ = -1;  // 当前最高层级
         uint64_t entry_point_ = std::numeric_limits<uint64_t>::max();
         std::unordered_map<uint64_t, Node> nodes_;  // uid -> Node
         std::mt19937 rng_{std::random_device{}()};
       
         // ---------- 内部工具 ----------
         float l2_distance(const VectorRecord& a, const VectorRecord& b) const;
         void search_layer(const VectorRecord& q, std::priority_queue<Neighbor>& top_candidates, int layer, int ef) const;
         int random_level();
       };
}  // namespace candy
