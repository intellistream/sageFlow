//
// Created by Pygon on 25-4-18.
//
#include "concurrency/concurrency_manager.h"

#include "concurrency/blank_controller.h"
#include "index/knn.h"

candy::ConcurrencyManager::ConcurrencyManager() = default;

candy::ConcurrencyManager::~ConcurrencyManager() = default;

auto candy::ConcurrencyManager::create_index(const std::string& name, const IndexType& index_type, int dimension)
    -> int {
  std::shared_ptr<Index> index = nullptr;
  switch (index_type) {
    case IndexType::None:
      return -1;
    case IndexType::HNSW:
      return -1;
    case IndexType::BruteForce:
      index = std::make_shared<Knn>();
      break;
  }
  index->index_id_ = index_id_counter_++;
  index->index_type_ = index_type;
  index->dimension_ = dimension;

  index->storage_manager_ = storage_;

  const auto blank_controller = std::make_shared<BlankController>(index);

  controller_map_[index->index_id_] = blank_controller;
  index_map_[name] = IdWithType{index->index_id_, index_type};
  return index->index_id_;
}

auto candy::ConcurrencyManager::drop_index(const std::string& name) -> bool { return false; }

auto candy::ConcurrencyManager::insert(int index_id, std::unique_ptr<VectorRecord>& record) -> bool {
  const auto it = controller_map_.find(index_id);
  if (it == controller_map_.end()) {
    return false;
  }
  const auto& controller = it->second;
  return controller->insert(record);
}

auto candy::ConcurrencyManager::erase(int index_id, std::unique_ptr<VectorRecord>& record) -> bool {
  const auto it = controller_map_.find(index_id);
  if (it == controller_map_.end()) {
    return false;
  }
  const auto& controller = it->second;
  return controller->erase(record);
}

auto candy::ConcurrencyManager::query(int index_id, std::unique_ptr<VectorRecord>& record, int k)
    -> std::vector<std::unique_ptr<VectorRecord>> {
  const auto it = controller_map_.find(index_id);
  if (it == controller_map_.end()) {
    return {};
  }
  const auto& controller = it->second;
  return controller->query(record, k);
}