#include "index/hnsw.h"

#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace candy {

namespace {
template <typename T, typename S, typename C>
auto get_queue_content(const std::priority_queue<T, S, C>& q) -> std::vector<T> {
  std::vector<T> content;
  auto temp = q;
  while (!temp.empty()) {
    content.push_back(temp.top());
    temp.pop();
  }
  return content;
}
}  // namespace

HNSW::HNSW(int m, int ef_construction, int ef_search)
    : m_(m),
      ef_construction_(ef_construction),
      ef_search_(ef_search),
      entry_point_(std::numeric_limits<uint64_t>::max()),
      rng_(std::random_device{}()) {
  if (m_ <= 0) {
    throw std::invalid_argument("HNSW: M must be positive.");
  }
  if (ef_construction_ <= 0) {
    throw std::invalid_argument("HNSW: efConstruction must be positive.");
  }
  if (ef_search_ <= 0) {
    throw std::invalid_argument("HNSW: efSearch must be positive.");
  }
}

auto HNSW::getDistance(const VectorRecord& a, const VectorRecord& b) const -> double {
  if (!storage_manager_ || !storage_manager_->getEngine()) {
    return 0.0;
  }
  return storage_manager_->getEngine()->EuclideanDistance(a.data_, b.data_);
}

auto HNSW::insert(uint64_t uid) -> bool {
  std::lock_guard<std::mutex> guard(mutex_);
  auto record_ptr = storage_manager_->getVectorByUid(uid);
  if (!record_ptr) {
    return false;
  }
  const auto& record = *record_ptr;

  int level = random_level();
  uint64_t current_entry_point = entry_point_;

  if (current_entry_point != std::numeric_limits<uint64_t>::max()) {
    if (level < max_level_) {
      auto top_candidates = std::priority_queue<Neighbor>();
      top_candidates.push({current_entry_point, getDistance(record, *storage_manager_->getVectorByUid(current_entry_point))});

      for (int l = max_level_; l > level; --l) {
        search_layer(record, top_candidates, l, 1);
      }
      current_entry_point = top_candidates.top().id_;
    }
  }

  std::vector<std::vector<uint64_t>> neighbors_for_levels(level + 1);

  for (int l = std::min(level, max_level_); l >= 0; --l) {
    auto top_candidates = std::priority_queue<Neighbor>();
    top_candidates.push({current_entry_point, getDistance(record, *storage_manager_->getVectorByUid(current_entry_point))});
    search_layer(record, top_candidates, l, ef_construction_);

    auto neighbors = select_neighbors_heuristic(record, get_queue_content(top_candidates), m_, l, false, false);
    neighbors_for_levels[l] = neighbors;

    for (uint64_t neighbor_id : neighbors) {
        if (nodes_.contains(neighbor_id)) {
            auto& neighbor_node = nodes_.at(neighbor_id);
            if (l < neighbor_node.links_.size()) {
                auto& neighbor_links = neighbor_node.links_[l];

                neighbor_links.push_back(uid);
                // Prune connections if necessary
                if (neighbor_links.size() > m_) {
                    // This part needs a proper implementation based on HNSW algorithm
                }
            }
        }
    }
  }

  Node new_node;
  new_node.id_ = uid;
  new_node.level_ = level;
  new_node.links_.resize(level + 1);
  for(int l = 0; l <= level; ++l) {
      if(l < neighbors_for_levels.size()) {
          new_node.links_[l] = neighbors_for_levels[l];
      }
  }
  nodes_.emplace(uid, std::move(new_node));

  if (level > max_level_) {
    max_level_ = level;
    entry_point_ = uid;
  }

  return true;
}

void HNSW::search_layer(const VectorRecord& q, std::priority_queue<Neighbor>& top_candidates, int layer, int ef) const {
  std::unordered_set<uint64_t> visited;
  std::priority_queue<Neighbor, std::vector<Neighbor>, std::greater<>> candidates;

  for (const auto& top : get_queue_content(top_candidates)) {
    candidates.push(top);
    visited.insert(top.id_);
  }

  while (!candidates.empty()) {
    auto current = candidates.top();
    candidates.pop();

    if (top_candidates.size() >= static_cast<size_t>(ef) && current.dist_ > top_candidates.top().dist_) {
      break;
    }

    const auto& node_iter = nodes_.find(current.id_);
    if (node_iter == nodes_.end() || layer >= node_iter->second.links_.size()) {
        continue;
    }
    const auto& node = node_iter->second;

    for (uint64_t neighbor_id : node.links_[layer]) {
      if (visited.find(neighbor_id) == visited.end()) {
        visited.insert(neighbor_id);
        auto neighbor_vec_ptr = storage_manager_->getVectorByUid(neighbor_id);
        if (!neighbor_vec_ptr) { continue;
}
        double dist = getDistance(q, *neighbor_vec_ptr);
        if (top_candidates.size() < static_cast<size_t>(ef) || dist < top_candidates.top().dist_) {
          top_candidates.push({neighbor_id, dist});
          candidates.push({neighbor_id, dist});
          if (top_candidates.size() > static_cast<size_t>(ef)) {
            top_candidates.pop();
          }
        }
      }
    }
  }
}

auto HNSW::random_level() -> int {
  if (m_ <= 1) {
    return 0;
  }
  double ml = 1.0 / std::log(static_cast<double>(m_));
  std::uniform_real_distribution<double> dis(0.0, 1.0);
  double r = dis(rng_);
  if (r == 0.0) {
    r = std::nextafter(0.0, 1.0);
  }
  return static_cast<int>(std::floor(-std::log(r) * ml));
}

auto HNSW::select_neighbors_basic(const VectorRecord& q, const std::vector<uint64_t>& candidates, int m) const
    -> std::vector<uint64_t> {
  std::priority_queue<Neighbor> working_queue;
  for (uint64_t id : candidates) {
    auto vec_ptr = storage_manager_->getVectorByUid(id);
    if(vec_ptr) {
        working_queue.push({id, getDistance(q, *vec_ptr)});
}
  }

  std::vector<uint64_t> result;
  while (result.size() < static_cast<size_t>(m) && !working_queue.empty()) {
    result.push_back(working_queue.top().id_);
    working_queue.pop();
  }
  return result;
}

auto HNSW::select_neighbors_heuristic(const VectorRecord& q, const std::vector<Neighbor>& candidates, int m, int lc,
                                      bool extend_candidates, bool keep_pruned_connections) const -> std::vector<uint64_t> {
  std::priority_queue<Neighbor, std::vector<Neighbor>, std::greater<>> working_queue;
  for (const auto& neighbor : candidates) {
    working_queue.push(neighbor);
  }

  if (extend_candidates) {
    std::unordered_set<uint64_t> visited_ids;
    for(const auto& n : candidates) { visited_ids.insert(n.id_);
}

    auto candidates_copy = candidates;
    for (const auto& e : candidates_copy) {
      const auto& node_iter = nodes_.find(e.id_);
      if (node_iter == nodes_.end() || lc >= node_iter->second.links_.size()) { continue;
}
      
      for (uint64_t e_adj_id : node_iter->second.links_[lc]) {
        if (visited_ids.find(e_adj_id) == visited_ids.end()) {
          visited_ids.insert(e_adj_id);
          auto vec_ptr = storage_manager_->getVectorByUid(e_adj_id);
          if(vec_ptr) {
            working_queue.push({e_adj_id, getDistance(q, *vec_ptr)});
}
        }
      }
    }
  }

  std::vector<uint64_t> result;
  std::priority_queue<Neighbor> pruned_connections; 

  while (!working_queue.empty()) {
      const auto& top = working_queue.top();
      working_queue.pop();

      if (result.empty()) {
          result.push_back(top.id_);
      } else {
          bool is_closer_than_all = true;
          for(auto r_id : result) {
              auto vec_ptr = storage_manager_->getVectorByUid(r_id);
              if(vec_ptr && getDistance(*storage_manager_->getVectorByUid(top.id_), *vec_ptr) < top.dist_) {
                  is_closer_than_all = false;
                  break;
              }
          }
          if(is_closer_than_all) {
              result.push_back(top.id_);
          } else {
              pruned_connections.push(top);
          }
      }
      if (result.size() >= static_cast<size_t>(m)) { break;
}
  }

  if (keep_pruned_connections) {
    while (result.size() < static_cast<size_t>(m) && !pruned_connections.empty()) {
      result.push_back(pruned_connections.top().id_);
      pruned_connections.pop();
    }
  }

  return result;
}

auto HNSW::query(std::unique_ptr<VectorRecord>& record, int k) -> std::vector<uint64_t> {
  std::lock_guard<std::mutex> guard(mutex_);
  if (entry_point_ == std::numeric_limits<uint64_t>::max()) {
    return {};
  }

  const auto& q = *record;
  auto top_candidates = std::priority_queue<Neighbor>();
  double dist = getDistance(q, *storage_manager_->getVectorByUid(entry_point_));
  top_candidates.push({entry_point_, dist});

  for (int l = max_level_; l > 0; --l) {
    search_layer(q, top_candidates, l, 1);
  }
  search_layer(q, top_candidates, 0, ef_search_);

  std::vector<uint64_t> result;
  while (result.size() < static_cast<size_t>(k) && !top_candidates.empty()) {
    result.push_back(top_candidates.top().id_);
    top_candidates.pop();
  }
  std::ranges::reverse(result);
  return result;
}

auto HNSW::erase(uint64_t uid) -> bool {
  std::lock_guard<std::mutex> guard(mutex_);
  auto node_iter = nodes_.find(uid);
  if (node_iter == nodes_.end()) {
    return false;
  }

  auto& node_to_remove = node_iter->second;
  int level = node_to_remove.level_;

  for (int l = 0; l <= level; ++l) {
    if (l >= node_to_remove.links_.size()) { continue;
}
    for (uint64_t neighbor_id : node_to_remove.links_[l]) {
      auto neighbor_iter = nodes_.find(neighbor_id);
      if (neighbor_iter != nodes_.end()) {
        auto& neighbor_node = neighbor_iter->second;
        if (l < neighbor_node.links_.size()) {
            auto& neighbor_links = neighbor_node.links_[l];
            neighbor_links.erase(std::remove(neighbor_links.begin(), neighbor_links.end(), uid), neighbor_links.end());
        }
      }
    }
  }

  nodes_.erase(uid);

  if (entry_point_ == uid) {
    if (!nodes_.empty()) {
      entry_point_ = nodes_.begin()->first;
    } else {
      entry_point_ = std::numeric_limits<uint64_t>::max();
      max_level_ = -1;
    }
  }

  return true;
}

}  // namespace candy