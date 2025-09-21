//
// Created by Pygon on 25-4-18.
//
#include "concurrency/concurrency_manager.h"

#include "concurrency/blank_controller.h"
#include "index/hnsw.h"
#include "index/ivf.h"
#include "index/knn.h"
#include "index/vectraflow.h"

candy::ConcurrencyManager::ConcurrencyManager(std::shared_ptr<StorageManager> storage) : storage_(std::move(storage)) {}

candy::ConcurrencyManager::~ConcurrencyManager() = default;

auto candy::ConcurrencyManager::create_index(const std::string& name, const IndexType& index_type, int dimension)
    -> int {
  std::shared_ptr<Index> index = nullptr;
  switch (index_type) {
    case IndexType::None:
      return -1;
    case IndexType::IVF:
      index = std::make_shared<Ivf>();
      break;
    case IndexType::HNSW:
      index = std::make_shared<HNSW>();
      break;
    case IndexType::Vectraflow:
      index = std::make_shared<VectraFlow>();
      break;
    case IndexType::BruteForce:
    default:
      index = std::make_shared<Knn>();
      break;
  }
  index->index_id_ = index_id_counter_++;
  index->index_type_ = index_type;
  index->dimension_ = dimension;

  index->storage_manager_ = storage_;
  storage_->engine_ = std::make_shared<ComputeEngine>();

  const auto blank_controller = std::make_shared<BlankController>(index);

  controller_map_[index->index_id_] = blank_controller;
  index_map_[name] = IdWithType{.id_ = index->index_id_, .index_type_ = index_type};
  return index->index_id_;
}

auto candy::ConcurrencyManager::create_index(const std::string& name, int dimension) -> int {
  return create_index(name, IndexType::BruteForce, dimension);
}

auto candy::ConcurrencyManager::drop_index(const std::string& name) -> bool { return false; }

auto candy::ConcurrencyManager::insert(int index_id, std::unique_ptr<VectorRecord> record) -> bool {
  const auto it = controller_map_.find(index_id);
  if (it == controller_map_.end()) {
    return false;
  }
  const auto& controller = it->second;
  return controller->insert(std::move(record));
}

auto candy::ConcurrencyManager::erase(int index_id, std::unique_ptr<VectorRecord> record) -> bool {
  const auto it = controller_map_.find(index_id);
  if (it == controller_map_.end()) {
    return false;
  }
  const auto& controller = it->second;
  return controller->erase(std::move(record));
}

auto candy::ConcurrencyManager::erase(int index_id, uint64_t uid) -> bool {
  const auto it = controller_map_.find(index_id);
  if (it == controller_map_.end()) {
    return false;
  }
  const auto& controller = it->second;
  return controller->erase(uid);
}

auto candy::ConcurrencyManager::query(int index_id, const VectorRecord& record, int k)
    -> std::vector<std::shared_ptr<const VectorRecord>> {
  const auto it = controller_map_.find(index_id);
  if (it == controller_map_.end()) {
    return {};
  }
  const auto& controller = it->second;
  return controller->query(record, k);
}

auto candy::ConcurrencyManager::query_for_join(int index_id, const VectorRecord& record,
                      double join_similarity_threshold) -> std::vector<std::shared_ptr<const VectorRecord>> {
  const auto it = controller_map_.find(index_id);
  if (it == controller_map_.end()) {
    return {};
  }
  const auto& controller = it->second;
  return controller->query_for_join(record, join_similarity_threshold);
}
