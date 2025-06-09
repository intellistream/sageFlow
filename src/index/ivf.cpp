#include "index/ivf.h"
#include <algorithm>
#include <limits>
#include <random>
#include <unordered_set>
#include <iostream>
#include <utility>

namespace candy {

Ivf::Ivf(int nlist, double rebuild_threshold, int nprobes)
    : nlist_(nlist),
      rebuild_threshold_(rebuild_threshold),
      vectors_since_last_rebuild_(0),
      nprobes_(nprobes) {
    // Initialize empty centroids and inverted lists
    centroids_.clear();
    inverted_lists_.clear();
}

Ivf::~Ivf() = default;

auto Ivf::assignToCluster(const VectorData& vec) -> int { // Use trailing return type
    if (centroids_.empty()) {
        return -1; // No clusters yet
    }
    
    int best_cluster = 0;
    double min_distance = std::numeric_limits<double>::max();
    
    for (size_t i = 0; i < centroids_.size(); ++i) {
        double distance = storage_manager_->engine_->EuclideanDistance(vec, centroids_[i]);
        if (distance < min_distance) {
            min_distance = distance;
            best_cluster = static_cast<int>(i);
        }
    }
    
    return best_cluster;
}

void Ivf::rebuildClusters() {
    if (storage_manager_->records_.empty()) {
        return;
    }
    auto t = static_cast<int>(sqrt(size_));
    nlist_ = std::max(nlist_, t);

    int actual_clusters = std::min(nlist_, size_);
    // Initialize centroids with random vectors from the dataset
    centroids_.clear();
    std::vector<int> selected_indices;
    
    // Random generator
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(0, size_ - 1);
    
    // Select random initial centroids
    while (centroids_.size() < actual_clusters) {
        int idx = distrib(gen);
        if (std::find(selected_indices.begin(), selected_indices.end(), idx) == selected_indices.end()) {
            selected_indices.push_back(idx);
            // Create a deep copy of the vector data for the centroid
            centroids_.push_back(storage_manager_->records_[idx]->data_);
        }
    }

    // K-means iterations
    const int MAX_ITERATIONS = 20;
    bool changed = true;
    int iteration = 0;
    
    std::vector<int> assignments(size_, -1);

    while (changed && iteration < MAX_ITERATIONS) {
        changed = false;
        
        // Assign each vector to nearest centroid
        for (int i = 0; i < size_; ++i) {
            double min_dist = std::numeric_limits<double>::max();
            int best_cluster = -1;
            
            for (int j = 0; j < actual_clusters; ++j) {
                double dist = storage_manager_->engine_->EuclideanDistance(
                    storage_manager_->records_[i]->data_, centroids_[j]);
                
                if (dist < min_dist) {
                    min_dist = dist;
                    best_cluster = j;
                }
            }
            
            if (assignments[i] != best_cluster) {
                assignments[i] = best_cluster;
                changed = true;
            }
        }
        
        // Recalculate centroids - need to work with raw data according to data_types.h
        std::vector<int> cluster_sizes(actual_clusters, 0);
        std::vector<VectorData> new_centroids;
        new_centroids.reserve(actual_clusters);
        
        // Initialize new centroids with zeros
        for (int j = 0; j < actual_clusters; ++j) {
            // Create new VectorData with the same dimension and type as our data
            DataType type = storage_manager_->records_[0]->data_.type_;
            int32_t dim = storage_manager_->records_[0]->data_.dim_;
            VectorData new_centroid(dim, type);

            // Initialize with zeros
            if (type == DataType::Float32) {
                auto* data_ptr = reinterpret_cast<float*>(new_centroid.data_.get());
                for (int d = 0; d < dim; ++d) {
                    data_ptr[d] = 0.0f;
                }
            } else if (type == DataType::Float64) {
                auto* data_ptr = reinterpret_cast<double*>(new_centroid.data_.get()); // Use auto
                for (int d = 0; d < dim; ++d) {
                    data_ptr[d] = 0.0;
                }
            }
            
            new_centroids.push_back(std::move(new_centroid));
        }
        
        // Sum vectors for each cluster
        for (int i = 0; i < size_; ++i) {
            int cluster = assignments[i];
            if (cluster >= 0) {
                const auto& record = storage_manager_->records_[i];
                DataType type = record->data_.type_;
                int32_t dim = record->data_.dim_;
                
                // Add this vector to the centroid sum
                if (type == DataType::Float32) {
                    auto* centroid_ptr = reinterpret_cast<float*>(new_centroids[cluster].data_.get()); // Use auto
                    auto* vector_ptr = reinterpret_cast<float*>(record->data_.data_.get()); // Use auto
                    for (int d = 0; d < dim; ++d) {
                        centroid_ptr[d] += vector_ptr[d];
                    }
                } else if (type == DataType::Float64) {
                    auto* centroid_ptr = reinterpret_cast<double*>(new_centroids[cluster].data_.get()); // Use auto
                    auto* vector_ptr = reinterpret_cast<double*>(record->data_.data_.get()); // Use auto
                    for (int d = 0; d < dim; ++d) {
                        centroid_ptr[d] += vector_ptr[d];
                    }
                }
                
                cluster_sizes[cluster]++;
            }
        }
        
        // Normalize centroids by dividing by cluster size
        for (int j = 0; j < actual_clusters; ++j) {
            if (cluster_sizes[j] > 0) {
                DataType type = new_centroids[j].type_;
                int32_t dim = new_centroids[j].dim_;
                
                if (type == DataType::Float32) {
                    auto* data_ptr = reinterpret_cast<float*>(new_centroids[j].data_.get()); // Use auto
                    for (int d = 0; d < dim; ++d) {
                        data_ptr[d] /= static_cast<float>(cluster_sizes[j]); // Explicit cast
                    }
                } else if (type == DataType::Float64) {
                    auto* data_ptr = reinterpret_cast<double*>(new_centroids[j].data_.get()); // Use auto
                    for (int d = 0; d < dim; ++d) {
                        data_ptr[d] /= static_cast<double>(cluster_sizes[j]); // Explicit cast
                    }
                }
            }
        }
        // Update centroids
        centroids_ = std::move(new_centroids);
        iteration++;
    }
    
    // Rebuild inverted lists
    inverted_lists_.clear();
    for (int i = 0; i < size_; ++i) {
        int cluster = assignments[i];
        if (cluster >= 0) {
            uint64_t id = storage_manager_->records_[i]->uid_;
            inverted_lists_[cluster].push_back(id);
        }
    }
    
    vectors_since_last_rebuild_ = 0;
}

auto Ivf::insert(uint64_t id) -> bool {
    // Get the vector record from storage manager
    auto record = storage_manager_->getVectorByUid(id);
    if (!record) {
        return false;
    }
    
    // On first insert, set the dimension
    if (dimension_ == 0) {
        dimension_ = record->data_.dim_;
    }
    
    // If no clusters yet or reached rebuild threshold, rebuild clusters
    if (centroids_.empty() || vectors_since_last_rebuild_ >= rebuild_threshold_* size_) {
        rebuildClusters();
    } else {
        // Otherwise, just assign to nearest cluster
        int cluster = assignToCluster(record->data_);
        if (cluster >= 0) {
            inverted_lists_[cluster].push_back(id);
        }
    }
    size_++;
    vectors_since_last_rebuild_++;
    return true;
}

auto Ivf::erase(uint64_t id) -> bool {
    bool found = false;
    
    // Remove from inverted lists
    for (auto& [cluster_id, ids] : inverted_lists_) {
        auto it = std::find(ids.begin(), ids.end(), id);
        if (it != ids.end()) {
            ids.erase(it);
            found = true;
            break;
        }
    }
    
    // Consider rebuilding if too many vectors have been removed
    if (found) {
        vectors_since_last_rebuild_--;
        size_--;
    }
    
    return found;
}

auto Ivf::query(std::unique_ptr<VectorRecord>& record, int k) -> std::vector<uint64_t> {
    if (centroids_.empty()) {
        return {};
    }

    // Find the closest nprobes_ clusters
    std::vector<std::pair<int, double>> cluster_distances;
    for (size_t i = 0; i < centroids_.size(); ++i) {
        double distance = storage_manager_->engine_->EuclideanDistance(record->data_, centroids_[i]);
        cluster_distances.emplace_back(static_cast<int>(i), distance);
    }

    // Sort clusters by distance
    std::sort(cluster_distances.begin(), cluster_distances.end(), [](const auto& a, const auto& b) {
        return a.second < b.second;
    });

    // Probe the top nprobes_ clusters
    std::vector<std::pair<uint64_t, double>> results;
    for (size_t i = 0; i < cluster_distances.size() && (std::cmp_less(results.size(), k) || std::cmp_less(i, this->nprobes_)); ++i) { // Explicit cast for comparison
        int cluster = cluster_distances[i].first;
        if (inverted_lists_.contains(cluster)) {
            const auto& candidate_ids = inverted_lists_.at(cluster);
            for (const auto& id_val : candidate_ids) {
                auto candidate = storage_manager_->getVectorByUid(id_val);
                if (candidate) {
                    double distance = storage_manager_->engine_->EuclideanDistance(record->data_, candidate->data_);
                    results.emplace_back(id_val, distance);
                }
            }
        }
    }

    // Sort by distance
    std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
        return a.second < b.second;
    });
    // Return top-k IDs
    std::vector<uint64_t> top_ids;
    int result_count = std::min(k, static_cast<int>(results.size()));
    for (int i = 0; i < result_count; ++i) {
        top_ids.push_back(results[i].first);
    }

    return top_ids;
}

auto Ivf::query_for_join(std::unique_ptr<VectorRecord>& record, double join_similarity_threshold) -> std::vector<uint64_t> {
  if (centroids_.empty()) {
    return {};
  }

  // Find the closest nprobes_ clusters
  std::vector<std::pair<int, double>> cluster_distances;
  for (size_t i = 0; i < centroids_.size(); ++i) {
    double distance = storage_manager_->engine_->EuclideanDistance(record->data_, centroids_[i]);
    cluster_distances.emplace_back(static_cast<int>(i), distance);
  }

  // Sort clusters by distance
  std::sort(cluster_distances.begin(), cluster_distances.end(), [](const auto& a, const auto& b) {
      return a.second < b.second;
  });

  // Probe the top nprobes_ clusters
  constexpr double epsilon = 1e-6;
  std::vector<uint64_t> results;
  for (size_t i = 0; i < cluster_distances.size() && std::cmp_less(i , this->nprobes_); ++i) {
    int cluster = cluster_distances[i].first;
    if (inverted_lists_.contains(cluster)) {
      const auto& candidate_ids = inverted_lists_.at(cluster);
      for (const auto& id_val : candidate_ids) {
        if (auto candidate = storage_manager_->getVectorByUid(id_val)) {
          double similarity = storage_manager_->engine_->Similarity(record->data_, candidate->data_);
          if (similarity - join_similarity_threshold > epsilon) {
            results.emplace_back(id_val);
          }
        }
      }
    }
  }

  return results;
}
}  // namespace candy

