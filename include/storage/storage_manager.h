#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <shared_mutex>
#include <mutex>

#include "common/data_types.h"
#include "compute_engine/compute_engine.h"

namespace sageFlow {
using idx_t = int32_t;

class StorageManager {
 public:
  static constexpr int GLOBAL_SHARD = -1;

  std::shared_ptr<ComputeEngine> engine_ = nullptr;
  // Constructor
  StorageManager() = default;

  // Destructor
  ~StorageManager() = default;

  // ---- Shard lifecycle ----
  void createShard(int shard_id);
  void removeShard(int shard_id);
  bool hasShard(int shard_id) const;

  // ---- Data operations (shard_id defaults to GLOBAL_SHARD for backward compatibility) ----
  auto insert(std::unique_ptr<VectorRecord> record, int shard_id = GLOBAL_SHARD) -> void;

  auto insert(RecordView record, int shard_id = GLOBAL_SHARD) -> void;
  auto erase(uint64_t vector_id, int shard_id = GLOBAL_SHARD) -> bool;

  auto getVectorByUid(uint64_t vector_id, int shard_id = GLOBAL_SHARD) -> std::shared_ptr<const VectorRecord>;

  auto getVectorsByUids(const std::vector<uint64_t> &vector_ids, int shard_id = GLOBAL_SHARD) -> std::vector<std::shared_ptr<const VectorRecord>>;

  auto topk(const VectorRecord &record, int k, int shard_id = GLOBAL_SHARD) const -> std::vector<uint64_t>;

  auto similarityJoinQuery(const VectorRecord &record,
                           double join_similarity_threshold,
                           double similarity_alpha,
                           int shard_id = GLOBAL_SHARD) const -> std::vector<uint64_t>;

 private:
  struct Shard {
    std::vector<RecordView> records;
    std::unordered_map<uint64_t, int32_t> map;
    mutable std::shared_mutex mutex;
  };

  Shard global_shard_;
  std::unordered_map<int, std::unique_ptr<Shard>> shards_;
  mutable std::shared_mutex shards_map_mutex_;  // protects shards_ map structure only

  auto resolveShard(int shard_id) -> Shard*;
  auto resolveShard(int shard_id) const -> const Shard*;
};
}  // namespace sageFlow
