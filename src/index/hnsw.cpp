#include "index/hnsw.h"

#include <algorithm> // For std::min, std::max, std::sort, std::reverse, std::remove
#include <cmath>     // For std::log
#include <limits>    // For std::numeric_limits
#include <memory>    // For std::unique_ptr, std::shared_ptr
#include <queue>     // For std::priority_queue
#include <random>    // For std::mt19937, std::random_device, std::uniform_real_distribution
#include <stdexcept> // For std::runtime_error, std::invalid_argument, std::logic_error
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>    // For std::to_string in error messages

// Include necessary headers for VectorRecord and StorageManager definitions
// These might need adjustment based on your project structure.
// Assuming they are accessible, potentially via "index/index.h" or other includes.
#include "storage/storage_manager.h"



namespace candy {

// --- Constructor ---
// Based on the provided hnsw.h
HNSW::HNSW(int m, int ef_construction, int ef_search)
    : m_(m),
      ef_construction_(ef_construction),
      ef_search_(ef_search),
      max_level_(-1),
      entry_point_(std::numeric_limits<uint64_t>::max()),
      rng_(std::random_device{}())
      // storage_manager_ is not declared in the provided hnsw.h,
      // but is required for l2_distance. Assuming it's intended to be
      // part of the class or set externally. Add member if needed.
      // storage_manager_(nullptr) // Example if added
{
    if (m_ <= 0) {
        throw std::invalid_argument("HNSW: M must be positive.");
    }
    if (ef_construction_ <= 0) {
         throw std::invalid_argument("HNSW: efConstruction must be positive.");
    }
     if (ef_search_ <= 0) {
         throw std::invalid_argument("HNSW: efSearch must be positive.");
    }
     // Note: m_max0_ (max neighbors for level 0) is not in the header.
     // Using m_ for all levels, including level 0 pruning.
}

// --- Public API Implementation ---

// Add a method to set the storage manager if it's a member variable.
// This is crucial for functionality but missing from the provided header.
// void HNSW::setStorageManager(std::shared_ptr<StorageManager> storage_manager) {
//     storage_manager_ = storage_manager;
// }


// --- Private Helper Functions Implementation ---

// Assumes storage_manager_ is accessible (e.g., a member variable)
inline auto HNSW::l2_distance(const VectorRecord& a, const VectorRecord& b) const -> double {
    // Add member 'std::shared_ptr<StorageManager> storage_manager_;' to hnsw.h if needed
    // and initialize it (e.g., in constructor or via setStorageManager).
    if (!storage_manager_ || !storage_manager_->engine_) {
        throw std::runtime_error("HNSW::l2_distance: Storage manager or distance engine not available. Ensure storage_manager_ is set.");
    }
    return storage_manager_->engine_->EuclideanDistance(a.data_, b.data_);
}

inline auto HNSW::random_level() -> int {
    if (m_ <= 1) return 0;
    double ml = 1.0 / std::log(static_cast<double>(m_));
    std::uniform_real_distribution<double> dis(0.0, 1.0);
    double r = dis(rng_);
    if (r == 0.0) r = std::nextafter(0.0, 1.0); // Avoid log(0)
    int level = static_cast<int>(std::floor(-std::log(r) * ml));
    return level;
}

// Assumes STANDARD Neighbor comparison from hnsw.h:
// operator< means smaller distance is "less".
// Default priority_queue is max-heap (largest distance top).
// priority_queue with std::greater is min-heap (smallest distance top).
inline void HNSW::search_layer(const VectorRecord& q,
                               std::priority_queue<Neighbor>& results_heap, // Max-heap (keeps ef nearest)
                               int layer,
                               int ef) const {
    if (results_heap.empty()) return;
    if (!storage_manager_) throw std::runtime_error("HNSW::search_layer: Storage manager not set.");

    std::unordered_set<uint64_t> visited;
    // Min-heap for candidates (closest first)
    std::priority_queue<Neighbor, std::vector<Neighbor>, std::greater<Neighbor>> candidates_min_heap;

    // Initialize visited and candidates from initial results
    std::priority_queue<Neighbor> initial_results = results_heap;
    while (!initial_results.empty()) {
        const Neighbor& neighbor = initial_results.top();
        if (visited.insert(neighbor.id_).second) {
            candidates_min_heap.push(neighbor);
        }
        initial_results.pop();
    }

    while (!candidates_min_heap.empty()) {
        Neighbor current_candidate = candidates_min_heap.top();
        candidates_min_heap.pop();

        // Optimization: Stop if closest candidate is farther than farthest in full results heap
        if (results_heap.size() >= static_cast<size_t>(ef) && current_candidate.dist_ > results_heap.top().dist_) {
            break;
        }

        auto node_iter = nodes_.find(current_candidate.id_);
        if (node_iter != nodes_.end() && layer >= 0 && layer < static_cast<int>(node_iter->second.links_.size())) {
            for (uint64_t neighbor_id : node_iter->second.links_[layer]) {
                if (visited.insert(neighbor_id).second) {
                    const auto neighbor_vec_ptr = storage_manager_->getVectorByUid(neighbor_id);
                    if (!neighbor_vec_ptr) continue;
                    double dist = l2_distance(q, *neighbor_vec_ptr);

                    if (results_heap.size() < static_cast<size_t>(ef) || dist < results_heap.top().dist_) {
                        candidates_min_heap.push({neighbor_id, dist});
                        results_heap.push({neighbor_id, dist});
                        if (results_heap.size() > static_cast<size_t>(ef)) {
                            results_heap.pop();
                        }
                    }
                }
            }
        }
    }
}

// Select M best neighbors from candidates C using basic heap selection.
// Assumes STANDARD Neighbor comparison.
inline auto HNSW::select_neighbors_basic(const VectorRecord& q,
                                         const std::vector<uint64_t>& C,
                                         int M,
                                         [[maybe_unused]] int lc) const -> std::vector<uint64_t> {
    std::priority_queue<Neighbor> results_max_heap; // Max-heap
    if (!storage_manager_) throw std::runtime_error("HNSW::select_neighbors_basic: Storage manager not set.");
    if (M <= 0) return {};

    for (uint64_t candidate_id : C) {
        const auto candidate_vec_ptr = storage_manager_->getVectorByUid(candidate_id);
        if (!candidate_vec_ptr) continue;
        double dist = l2_distance(q, *candidate_vec_ptr);

        if (results_max_heap.size() < static_cast<size_t>(M)) {
            results_max_heap.push({candidate_id, dist});
        } else if (dist < results_max_heap.top().dist_) {
            results_max_heap.pop();
            results_max_heap.push({candidate_id, dist});
        }
    }

    std::vector<uint64_t> result_ids;
    result_ids.reserve(results_max_heap.size());
    while (!results_max_heap.empty()) {
        result_ids.push_back(results_max_heap.top().id_);
        results_max_heap.pop();
    }
    std::reverse(result_ids.begin(), result_ids.end()); // Nearest first
    return result_ids;
}

// Select M neighbors using heuristic algorithm.
// Assumes STANDARD Neighbor comparison.
inline auto HNSW::select_neighbors_heuristic(const VectorRecord& q,
                                             const std::vector<uint64_t>& C,
                                             int M,
                                             int lc,
                                             bool extend_candidates,
                                             bool keep_pruned_connections) const -> std::vector<uint64_t> {
     if (!storage_manager_) throw std::runtime_error("HNSW::select_neighbors_heuristic: Storage manager not set.");
     if (M <= 0) return {};

    std::priority_queue<Neighbor, std::vector<Neighbor>, std::greater<Neighbor>> W_min_heap; // Min-heap for candidates
    std::unordered_set<uint64_t> W_set;

    for (uint64_t start_node_id : C) {
        const auto vec_ptr = storage_manager_->getVectorByUid(start_node_id);
        if (!vec_ptr) continue;
        double dist = l2_distance(q, *vec_ptr);
        if (W_set.insert(start_node_id).second) {
             W_min_heap.push({start_node_id, dist});
        }
    }

    if (extend_candidates) {
        std::vector<uint64_t> initial_candidates = C;
        for (uint64_t e_id : initial_candidates) {
            auto node_iter = nodes_.find(e_id);
            if (node_iter != nodes_.end() && lc >= 0 && lc < static_cast<int>(node_iter->second.links_.size())) {
                for (uint64_t e_adj_id : node_iter->second.links_[lc]) {
                    if (W_set.insert(e_adj_id).second) {
                        const auto vec_ptr = storage_manager_->getVectorByUid(e_adj_id);
                        if (!vec_ptr) continue;
                        double dist = l2_distance(q, *vec_ptr);
                        W_min_heap.push({e_adj_id, dist});
                    }
                }
            }
        }
    }

    std::priority_queue<Neighbor> R_max_heap; // Max-heap for results
    std::priority_queue<Neighbor> W_pruned_max_heap; // Max-heap for pruned

    while (!W_min_heap.empty()) {
        Neighbor e = W_min_heap.top();
        W_min_heap.pop();

        if (R_max_heap.size() >= static_cast<size_t>(M) && e.dist_ > R_max_heap.top().dist_) {
             if (!keep_pruned_connections) break;
             W_pruned_max_heap.push(e);
             continue;
        }

        if (R_max_heap.size() < static_cast<size_t>(M)) {
            R_max_heap.push(e);
        } else {
            Neighbor removed = R_max_heap.top();
            R_max_heap.pop();
            R_max_heap.push(e);
            if (keep_pruned_connections) {
                W_pruned_max_heap.push(removed);
            }
        }
    }

    if (keep_pruned_connections) {
        while (!W_pruned_max_heap.empty() && R_max_heap.size() < static_cast<size_t>(M)) {
            R_max_heap.push(W_pruned_max_heap.top());
            W_pruned_max_heap.pop();
        }
    }

    std::vector<uint64_t> result_ids;
    result_ids.reserve(R_max_heap.size());
    while (!R_max_heap.empty()) {
        result_ids.push_back(R_max_heap.top().id_);
        R_max_heap.pop();
    }
    std::reverse(result_ids.begin(), result_ids.end()); // Nearest first
    return result_ids;
}


// --- Public API Implementation Continued ---

auto HNSW::insert(uint64_t uid) -> bool {
    if (!storage_manager_) throw std::runtime_error("HNSW::insert: Storage manager not set.");

    const auto rec_ptr = storage_manager_->getVectorByUid(uid);
    if (!rec_ptr) throw std::runtime_error("HNSW::insert: Vector data for UID " + std::to_string(uid) + " not found.");

    const VectorRecord& rec = *rec_ptr;

    // Use lock/atomic for concurrent access checks/modifications below
    if (nodes_.count(uid)) return false; // Node already exists

    const int cur_level = random_level();
    Node new_node{uid, cur_level, std::vector<std::vector<uint64_t>>(cur_level + 1)};

    uint64_t current_ep = entry_point_;
    int current_max_level = max_level_;

    if (current_ep == std::numeric_limits<uint64_t>::max()) { // Empty graph
        entry_point_ = uid;
        max_level_ = cur_level;
        nodes_.emplace(uid, std::move(new_node));
        return true;
    }

    const auto ep_vec_ptr = storage_manager_->getVectorByUid(current_ep);
    if (!ep_vec_ptr) throw std::runtime_error("HNSW::insert: Entry point vector UID " + std::to_string(current_ep) + " missing.");
    double ep_dist = l2_distance(rec, *ep_vec_ptr);

    // Phase 1: Find entry points from top level down to cur_level + 1
    for (int level = current_max_level; level > cur_level; --level) {
        bool changed = true;
        while (changed) {
            changed = false;
            uint64_t next_ep = current_ep;
            double min_dist = ep_dist;
            auto node_iter = nodes_.find(current_ep);
            if (node_iter == nodes_.end() || level < 0 || level >= static_cast<int>(node_iter->second.links_.size())) break;

            for (uint64_t neighbor_id : node_iter->second.links_[level]) {
                const auto neighbor_vec_ptr = storage_manager_->getVectorByUid(neighbor_id);
                if (!neighbor_vec_ptr) continue;
                double d = l2_distance(rec, *neighbor_vec_ptr);
                if (d < min_dist) {
                    min_dist = d; next_ep = neighbor_id; changed = true;
                }
            }
            current_ep = next_ep;
            const auto current_ep_vec_ptr_updated = storage_manager_->getVectorByUid(current_ep);
            if (!current_ep_vec_ptr_updated) throw std::runtime_error("HNSW::insert: Vector for updated entry point UID " + std::to_string(current_ep) + " missing.");
            ep_dist = l2_distance(rec, *current_ep_vec_ptr_updated);
        }
    }

    // Phase 2: Insert from level cur_level down to 0
    uint64_t ep_for_level = current_ep;
    for (int level = std::min(cur_level, current_max_level); level >= 0; --level) {
        std::priority_queue<Neighbor> top_candidates_max_heap; // Max-heap for search results
        const auto ep_level_vec_ptr = storage_manager_->getVectorByUid(ep_for_level);
        if (!ep_level_vec_ptr) throw std::runtime_error("HNSW::insert: Entry point vector UID " + std::to_string(ep_for_level) + " for level " + std::to_string(level) + " missing.");
        top_candidates_max_heap.push({ep_for_level, l2_distance(rec, *ep_level_vec_ptr)});

        search_layer(rec, top_candidates_max_heap, level, ef_construction_);

        // Select M neighbors (using m_ for all levels as m_max0_ is not in header)
        int M = m_;
        std::vector<Neighbor> candidates_vec;
        candidates_vec.reserve(top_candidates_max_heap.size());
        while(!top_candidates_max_heap.empty()){ candidates_vec.push_back(top_candidates_max_heap.top()); top_candidates_max_heap.pop(); }
        std::reverse(candidates_vec.begin(), candidates_vec.end()); // Nearest first

        std::vector<uint64_t> candidate_ids;
        candidate_ids.reserve(candidates_vec.size());
        for(const auto& n : candidates_vec) candidate_ids.push_back(n.id_);

        // Use heuristic selection for potentially better graph structure
        std::vector<uint64_t> selected_neighbor_ids = select_neighbors_heuristic(rec, candidate_ids, M, level, true, false);

        // Connect new node
        if (level < static_cast<int>(new_node.links_.size())) {
            new_node.links_[level] = selected_neighbor_ids;
        } else {
            throw std::logic_error("HNSW::insert: new_node links vector size mismatch.");
        }

        // Connect neighbors back and prune
        for (uint64_t neighbor_id : selected_neighbor_ids) {
            auto neighbor_node_iter = nodes_.find(neighbor_id);
            if (neighbor_node_iter != nodes_.end()) {
                auto& neighbor_node = neighbor_node_iter->second;
                if (level >= static_cast<int>(neighbor_node.links_.size())) {
                     neighbor_node.links_.resize(level + 1);
                }
                auto& neighbor_links = neighbor_node.links_[level];
                neighbor_links.push_back(uid); // Add back link

                // Prune neighbor connections if exceeding M
                if (neighbor_links.size() > static_cast<size_t>(M)) {
                    const auto neighbor_vec_ptr = storage_manager_->getVectorByUid(neighbor_id);
                    if (neighbor_vec_ptr) {
                        std::vector<uint64_t> current_neighbor_ids = neighbor_links;
                        std::vector<uint64_t> pruned_neighbor_ids = select_neighbors_heuristic(*neighbor_vec_ptr, current_neighbor_ids, M, level, true, false);
                        neighbor_links = pruned_neighbor_ids;
                    } // else: cannot prune without vector data
                }
            } // else: neighbor node not found? inconsistency.
        }
        if (!candidates_vec.empty()) ep_for_level = candidates_vec[0].id_; // Update entry for next level
    }

    // Add the new node to the map
    nodes_.emplace(uid, std::move(new_node));

    // Phase 3: Update global entry point/max level if needed
    if (cur_level > current_max_level) {
        max_level_ = cur_level;
        entry_point_ = uid;
    }

    return true;
}


auto HNSW::erase(uint64_t uid) -> bool {
    // Use lock for concurrent access
    auto node_iter = nodes_.find(uid);
    if (node_iter == nodes_.end()) return false; // Not found

    const Node& node_to_erase = node_iter->second;
    int erased_node_level = node_to_erase.level_;
    std::vector<std::vector<uint64_t>> links_copy = node_to_erase.links_;

    // Remove incoming links
    for (int level = 0; level <= erased_node_level; ++level) {
        if (level >= static_cast<int>(links_copy.size())) continue;
        for (uint64_t neighbor_id : links_copy[level]) {
            auto neighbor_iter = nodes_.find(neighbor_id);
            if (neighbor_iter != nodes_.end() && level < static_cast<int>(neighbor_iter->second.links_.size())) {
                auto& neighbor_links = neighbor_iter->second.links_[level];
                neighbor_links.erase(std::remove(neighbor_links.begin(), neighbor_links.end(), uid), neighbor_links.end());
            }
        }
    }

    // Remove the node
    nodes_.erase(node_iter);

    // Update entry point / max level
    bool need_max_level_recalc = false;
    if (entry_point_ == uid) {
        if (nodes_.empty()) {
            entry_point_ = std::numeric_limits<uint64_t>::max(); max_level_ = -1;
        } else {
            uint64_t new_entry_point = nodes_.begin()->first; int new_max_level = -1;
            for (const auto& pair : nodes_) {
                if (pair.second.level_ > new_max_level) {
                    new_max_level = pair.second.level_; new_entry_point = pair.first;
                }
            }
            entry_point_ = new_entry_point; max_level_ = new_max_level;
        }
    } else if (!nodes_.empty() && erased_node_level == max_level_) {
        bool found_other_at_max = false;
        for (const auto& pair : nodes_) { if (pair.second.level_ == max_level_) { found_other_at_max = true; break; } }
        if (!found_other_at_max) need_max_level_recalc = true;
    } else if (nodes_.empty()) {
         entry_point_ = std::numeric_limits<uint64_t>::max(); max_level_ = -1;
    }

    if (need_max_level_recalc) {
        int current_max_level = -1;
        for (const auto& pair : nodes_) current_max_level = std::max(current_max_level, pair.second.level_);
        max_level_ = current_max_level;
    }

    return true;
}


auto HNSW::query(std::unique_ptr<VectorRecord>& record, int k) -> std::vector<uint64_t> {
    if (!storage_manager_) throw std::runtime_error("HNSW::query: Storage manager not set.");
    if (!record) throw std::invalid_argument("HNSW::query: Query record is null.");
    const VectorRecord& q = *record;

    // Use lock for concurrent access
    uint64_t current_ep = entry_point_;
    int current_max_level = max_level_;

    if (current_ep == std::numeric_limits<uint64_t>::max()) return {}; // Empty graph
    if (k <= 0) return {}; // Invalid k

    const auto ep_vec_ptr = storage_manager_->getVectorByUid(current_ep);
    if (!ep_vec_ptr) throw std::runtime_error("HNSW::query: Entry point vector UID " + std::to_string(current_ep) + " missing.");
    double ep_dist = l2_distance(q, *ep_vec_ptr);

    // Phase 1: Greedy search down to level 1
    for (int level = current_max_level; level > 0; --level) {
        bool changed = true;
        while (changed) {
            changed = false;
            uint64_t next_ep = current_ep; double min_dist = ep_dist;
            auto node_iter = nodes_.find(current_ep);
            if (node_iter == nodes_.end() || level < 0 || level >= static_cast<int>(node_iter->second.links_.size())) break;

            for (uint64_t neighbor_id : node_iter->second.links_[level]) {
                const auto neighbor_vec_ptr = storage_manager_->getVectorByUid(neighbor_id);
                if (!neighbor_vec_ptr) continue;
                double d = l2_distance(q, *neighbor_vec_ptr);
                if (d < min_dist) { min_dist = d; next_ep = neighbor_id; changed = true; }
            }
            current_ep = next_ep;
            const auto current_ep_vec_ptr_updated = storage_manager_->getVectorByUid(current_ep);
            if (!current_ep_vec_ptr_updated) throw std::runtime_error("HNSW::query: Vector for updated entry point UID " + std::to_string(current_ep) + " missing.");
            ep_dist = l2_distance(q, *current_ep_vec_ptr_updated);
        }
    }

    // Phase 2: Search layer 0
    std::priority_queue<Neighbor> top_candidates_max_heap; // Max-heap for results
    const auto final_ep_vec_ptr = storage_manager_->getVectorByUid(current_ep);
    if (!final_ep_vec_ptr) throw std::runtime_error("HNSW::query: Entry point vector UID " + std::to_string(current_ep) + " for layer 0 search is invalid.");
    top_candidates_max_heap.push({current_ep, l2_distance(q, *final_ep_vec_ptr)});

    search_layer(q, top_candidates_max_heap, 0, std::max(ef_search_, k));

    // Phase 3: Extract top K
    std::vector<uint64_t> final_result_ids;
    final_result_ids.reserve(k);
    std::vector<Neighbor> results_temp;
    results_temp.reserve(top_candidates_max_heap.size());
    while (!top_candidates_max_heap.empty()) { results_temp.push_back(top_candidates_max_heap.top()); top_candidates_max_heap.pop(); }
    std::reverse(results_temp.begin(), results_temp.end()); // Nearest first

    int count = 0;
    for (const auto& neighbor : results_temp) {
        if (count >= k) break;
        final_result_ids.push_back(neighbor.id_);
        count++;
    }

    return final_result_ids;
}


} // namespace candy