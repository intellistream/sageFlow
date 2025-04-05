#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "compute_engine/compute_engine.h"
#include "storage/storage_engine.h"

namespace candy {
enum class IndexType {  // NOLINT
  None,
  HNSW,
  BruteForce,
};

class AbstractIndex {
 public:
  // data
  IndexType index_type_;
  std::shared_ptr<AbstractStorageEngine> storage_engine_ = nullptr;
  std::shared_ptr<ComputeEngine> compute_engine_ = nullptr;

  // Constructor
  AbstractIndex() = default;
  // Destructor
  virtual ~AbstractIndex() = default;

  virtual auto insert(std::unique_ptr<VectorRecord> &record) -> bool;
  virtual auto insert(std::unique_ptr<VectorData> &record, uint64_t uid_t) -> bool;
  virtual auto insert(std::vector<std::unique_ptr<VectorRecord>> &records) -> bool;
  virtual auto erase(uint64_t vector_id) -> bool;
  virtual auto erase(std::vector<uint64_t> &vector_ids) -> bool;
  virtual auto query(std::unique_ptr<VectorRecord> &record, int k) -> std::vector<std::unique_ptr<VectorRecord>>;

  virtual auto build() -> bool;
  virtual auto save(const std::string &path) -> bool;
  virtual auto load(const std::string &path) -> bool;

  auto getVectorById(uint64_t vector_id) -> std::unique_ptr<VectorRecord>;
  auto getSize() -> size_t;
  auto getIndexType() -> IndexType;
  auto getStorageEngine() -> std::shared_ptr<AbstractStorageEngine> &;
  auto setStorageEngine(std::shared_ptr<AbstractStorageEngine> storage_engine) -> void;
};

class GlobalIndex {
 public:
  std ::shared_ptr<AbstractIndex> index_ = nullptr;

  GlobalIndex() = default;

  virtual auto insert(std::unique_ptr<VectorRecord> &record) -> bool;
  virtual auto insert(std::unique_ptr<VectorData> &record, uint64_t uid) -> bool;
  virtual auto insert(std::vector<std::unique_ptr<VectorRecord>> &records) -> bool;
  virtual auto erase(uint64_t vector_id) -> bool;
  virtual auto erase(std::vector<uint64_t> &vector_ids) -> bool;
  virtual auto query(std::unique_ptr<VectorRecord> &record, int k) -> std::vector<std::unique_ptr<VectorRecord>>;
  auto getVectorById(uint64_t vector_id) -> std::unique_ptr<VectorRecord>;

  auto buildIndex() -> bool;
  auto needReIndex() -> bool;
  auto chooseIndexType() -> IndexType;
  auto save(const std::string &path) -> bool;
  auto load(const std::string &path) -> bool;

  // auto readConfiguration(const std::string &config_file_path) -> ConfigMap;
};
}  // namespace candy