#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <variant>
#include <optional>

#include "compute_engine/compute_engine.h"
#include "storage/storage_manager.h"

namespace sageFlow {
enum class IndexType {  // NOLINT
  None,
  HNSW,
  BruteForce,
  IVF,
  Vectraflow,
  HDRForest,
  HDRTree,
  PartitionedIndex,
  FaissIVF,
  FaissHNSW
};

// Index-specific parameter structures
struct IVFParameters {
  int nlist = 1000;
  double rebuild_threshold = 1.5;
  int nprobes = 10;
};

struct HDRForestParameters {
  int n_clusters = 10;
  int f_sections = 5;
};

struct HNSWParameters {
  int m = 20;
  int ef_construction = 100;
  int ef_search = 40;
};

struct FaissIVFParameters {
  int nlist = 100;
  int nprobe = 10;
};

struct FaissHNSWParameters {
  int M = 32;
  int efConstruction = 40;
  int efSearch = 16;
};

struct NoParameters {};

// Variant to hold any index parameters
using IndexParameters = std::variant<NoParameters, IVFParameters, HNSWParameters, HDRForestParameters, FaissIVFParameters, FaissHNSWParameters>;

class Index {
 public:
  // data
  int index_id_ = 0;
  int dimension_ = 0;

  IndexType index_type_;
  std::shared_ptr<StorageManager> storage_manager_ = nullptr;
  // Constructor
  Index() = default;
  // Destructor
  virtual ~Index() = default;

  virtual auto insert(uint64_t id) -> bool = 0;
  virtual auto erase(uint64_t id) -> bool = 0;
  virtual auto query(const VectorRecord &record, int k) -> std::vector<uint64_t> = 0;
  virtual auto query_for_join(const VectorRecord &record,
                              double join_similarity_threshold) -> std::vector<uint64_t> = 0;
};

class GlobalIndex final : public Index {
 public:
  auto save(const std::string &path) -> bool;
  auto load(const std::string &path) -> bool;
  auto remove() -> bool;
};
}  // namespace sageFlow
