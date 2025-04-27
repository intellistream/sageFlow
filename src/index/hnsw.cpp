#include "index/hnsw.h"

#include <cmath>
#include <limits>
#include <memory>
#include <queue>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace candy {

inline HNSW::HNSW(int m, int efConstruction, int efSearch)
    : M_(m), ef_construction_(efConstruction), ef_search_(efSearch) {}

inline float HNSW::l2_distance(const VectorRecord& a, const VectorRecord& b) const {
  return storage_manager_->engine_->EuclideanDistance(a.data_, b.data_);
}

inline int HNSW::random_level() {
  std::uniform_real_distribution<float> dis(0.0f, 1.0f);
  float p = 1.0f / std::log(static_cast<float>(M_));  // 常用参数 1/ln(M)
  int level = 0;
  while (dis(rng_) < p && level < 32) ++level;
  return level;
}

inline bool HNSW::insert(uint64_t uid) {
  if (!storage_manager_) return false;
  const auto rec = storage_manager_->getVectorByUid(uid);
  if (!rec) return false;

  const int cur_level = random_level();
  Node new_node{uid, cur_level, std::vector<std::vector<uint64_t>>(cur_level + 1)};

  if (entry_point_ == std::numeric_limits<uint64_t>::max()) {
    // 第一条向量
    entry_point_ = uid;
    max_level_ = cur_level;
    nodes_.emplace(uid, std::move(new_node));
    return true;
  }

  uint64_t ep = entry_point_;
  // 从最高层往下，寻找最佳入口
  for (int level = max_level_; level > cur_level; --level) {
    bool improved = true;
    while (improved) {
      improved = false;
      for (uint64_t nid : nodes_[ep].links[level]) {
        if (l2_distance(*rec, *storage_manager_->getVectorByUid(nid)) <
            l2_distance(*rec, *storage_manager_->getVectorByUid(ep))) {
          ep = nid;
          improved = true;
        }
      }
    }
  }

  // 在新节点的每一层建立邻接
  for (int level = std::min(cur_level, max_level_); level >= 0; --level) {
    std::priority_queue<Neighbor> top_candidates;
    top_candidates.push({ep, l2_distance(*rec, *storage_manager_->getVectorByUid(ep))});
    search_layer(*rec, top_candidates, level, ef_construction_);

    // 选择前 M_ 个最近邻
    std::vector<Neighbor> neighbors;
    while (!top_candidates.empty()) {
      neighbors.push_back(top_candidates.top());
      top_candidates.pop();
    }
    if (neighbors.size() > static_cast<size_t>(M_)) {
      std::partial_sort(neighbors.begin(), neighbors.begin() + M_, neighbors.end());
      neighbors.resize(M_);
    }

    // 建立双向连接
    for (auto const& nb : neighbors) {
      nodes_[nb.id].links[level].push_back(uid);
      new_node.links[level].push_back(nb.id);
    }
  }

  // 更新入口
  if (cur_level > max_level_) {
    max_level_ = cur_level;
    entry_point_ = uid;
  }

  nodes_.emplace(uid, std::move(new_node));
  return true;
}

inline void HNSW::search_layer(const VectorRecord& q, std::priority_queue<Neighbor>& top_candidates, int layer,
                               int ef) const {
  std::unordered_set<uint64_t> visited;
  std::priority_queue<Neighbor> candidates = top_candidates;  // 工作队列

  // Step 1: 扩展候选节点并选择邻居
  // 启发式方法：从当前候选节点中选择 M_ 个最接近 q 的邻居
  std::vector<uint64_t> extended_candidates = select_neighbors_heuristic(q, {top_candidates.top().id}, ef, layer,
                                                                         true,   // 扩展候选
                                                                         true);  // 保留丢弃的连接
  // 非启发式
  // std::vector<uint64_t> extended_candidates = select_neighbors_basic(q, {top_candidates.top().id}, ef, layer);

  for (uint64_t nid : extended_candidates) {
    if (!visited.insert(nid).second) continue;
    float dist = l2_distance(q, *storage_manager_->getVectorByUid(nid));
    if (static_cast<int>(candidates.size()) < ef || dist < candidates.top().dist) {
      candidates.push({nid, dist});
      if (static_cast<int>(candidates.size()) > ef) {
        candidates.pop();
      }
    }
  }

  // 将候选结果存入 top_candidates
  top_candidates = candidates;
}

inline bool HNSW::erase(uint64_t uid) {
  auto it = nodes_.find(uid);
  if (it == nodes_.end()) return false;

  // 删除所有层的引用
  for (int level = 0; level <= it->second.level; ++level) {
    for (uint64_t nb : it->second.links[level]) {
      auto& vec = nodes_[nb].links[level];
      std::erase(vec, uid);
    }
  }

  // 更新入口点（简单做法：若删的是入口，则随便取一个剩余节点）
  if (entry_point_ == uid) {
    if (nodes_.size() > 1) {
      entry_point_ = nodes_.begin()->first;
    } else {
      entry_point_ = std::numeric_limits<uint64_t>::max();
      max_level_ = -1;
    }
  }
  nodes_.erase(it);
  return true;
}

inline std::vector<uint64_t> HNSW::select_neighbors_basic(const VectorRecord& q, const std::vector<uint64_t>& C, int M,
                                                          int lc) const {
  std::vector<uint64_t> R;         // 存储选中的 M 个邻居
  std::unordered_set<uint64_t> W;  // 工作队列，存放候选节点
  W.insert(C.begin(), C.end());    // 初始候选节点

  // Step 1: 从当前候选节点 C 中选择距离最小的 M 个邻居
  while (W.size() > 0 && R.size() < static_cast<size_t>(M)) {
    // 提取距离 q 最近的元素 e
    uint64_t e = *std::min_element(W.begin(), W.end(), [&q, this](uint64_t a, uint64_t b) {
      return l2_distance(*storage_manager_->getVectorByUid(a), q) <
             l2_distance(*storage_manager_->getVectorByUid(b), q);
    });
    W.erase(e);  // 从 W 中移除 e

    // 如果 e 更接近 q，相较于 R 中的任何元素，则加入 R
    if (R.size() < static_cast<size_t>(M)) {
      R.push_back(e);
    }
  }

  return R;  // 返回选中的 M 个邻居
}

inline std::vector<uint64_t> HNSW::select_neighbors_heuristic(const VectorRecord& q, const std::vector<uint64_t>& C,
                                                              int M, int lc, bool extendCandidates,
                                                              bool keepPrunedConnections) const {
  std::vector<uint64_t> R;         // 存储选中的邻居
  std::unordered_set<uint64_t> W;  // 工作队列，存放候选节点
  W.insert(C.begin(), C.end());    // 初始候选节点

  // Step 1: 扩展候选节点的邻居
  if (extendCandidates) {
    for (uint64_t e : C) {
      auto const& e_neighborhood = nodes_.at(e).links[lc];  // 获取节点 e 的邻居
      for (uint64_t eadj : e_neighborhood) {
        if (W.find(eadj) == W.end()) {  // 如果 eadj 不在 W 中
          W.insert(eadj);               // 将 eadj 加入 W
        }
      }
    }
  }

  std::unordered_set<uint64_t> Wd;  // 丢弃的候选节点（那些不能加入 R 的）

  // Step 2: 从 W 中选择前 M 个最接近 q 的邻居
  while (W.size() > 0 && R.size() < static_cast<size_t>(M)) {
    // 提取距离 q 最近的元素 e
    uint64_t e = *std::min_element(W.begin(), W.end(), [&q, this](uint64_t a, uint64_t b) {
      return l2_distance(*storage_manager_->getVectorByUid(a), q) <
             l2_distance(*storage_manager_->getVectorByUid(b), q);
    });
    W.erase(e);  // 从 W 中移除 e

    // 如果 e 更接近 q，相较于 R 中的任何元素，则加入 R
    if (R.size() < static_cast<size_t>(M)) {
      R.push_back(e);
    } else {
      // 如果 e 不在 R 中，加入丢弃队列 Wd
      Wd.insert(e);
    }
  }

  // Step 3: 如果 keepPrunedConnections 为真，处理丢弃的连接（从 Wd 中选）
  if (keepPrunedConnections) {
    while (Wd.size() > 0 && R.size() < static_cast<size_t>(M)) {
      uint64_t e = *std::min_element(Wd.begin(), Wd.end(), [&q, this](uint64_t a, uint64_t b) {
        return l2_distance(*storage_manager_->getVectorByUid(a), q) <
               l2_distance(*storage_manager_->getVectorByUid(b), q);
      });
      Wd.erase(e);     // 从 Wd 中移除 e
      R.push_back(e);  // 将 e 加入 R
    }
  }

  return R;  // 返回选中的 M 个邻居
}

inline std::vector<uint64_t> HNSW::query(std::unique_ptr<VectorRecord>& record, int k) {
  if (entry_point_ == std::numeric_limits<uint64_t>::max()) return {};

  uint64_t ep = entry_point_;
  // 先从顶层向下贪婪搜索
  for (int level = max_level_; level > 0; --level) {
    bool improved = true;
    while (improved) {
      improved = false;
      for (uint64_t nid : nodes_[ep].links[level]) {
        if (l2_distance(*record, *storage_manager_->getVectorByUid(nid)) <
            l2_distance(*record, *storage_manager_->getVectorByUid(ep))) {
          ep = nid;
          improved = true;
        }
      }
    }
  }

  // 在 0 层做 ef_search_ 搜索
  std::priority_queue<Neighbor> top_candidates;
  top_candidates.push({ep, l2_distance(*record, *storage_manager_->getVectorByUid(ep))});
  search_layer(*record, top_candidates, 0, std::max(ef_search_, k));

  // 输出前 k 个 uid，按距离由近到远排序
  std::vector<Neighbor> tmp;
  while (!top_candidates.empty()) {
    tmp.push_back(top_candidates.top());
    top_candidates.pop();
  }
  std::ranges::sort(tmp, [](auto const& a, auto const& b) { return a.dist < b.dist; });

  std::vector<uint64_t> result;
  for (size_t i = 0; i < tmp.size() && static_cast<int>(i) < k; ++i) {
    result.push_back(tmp[i].id);  // 返回 storage 的下标
  }
  return result;
}

}  // namespace candy
