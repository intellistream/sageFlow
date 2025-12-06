//
// Created by Pygon on 25-4-18.
//
#include "concurrency/concurrency_manager.h"

#include "concurrency/blank_controller.h"
#include "index/hnsw.h"
#include "index/ivf.h"
#include "index/knn.h"
#include "index/vectraflow.h"
#include "index/hdr_forest.h"
#include "index/hdr_tree.h"

sageFlow::ConcurrencyManager::ConcurrencyManager(std::shared_ptr<StorageManager> storage) : storage_(std::move(storage)) {}

sageFlow::ConcurrencyManager::~ConcurrencyManager() = default;

auto sageFlow::ConcurrencyManager::create_index(const std::string& name, const IndexType& index_type, int dimension)
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
    case IndexType::HDRForest:
      index = std::make_shared<HDRForest>();
      break;
    case IndexType::HDRTree:
      index = std::make_shared<HDRTree>(dimension);
      break;
    case IndexType::BruteForce:
    default:
      index = std::make_shared<Knn>();
      break;
  }
  index->index_id_ = index_id_counter_++;
  index->index_type_ = index_type;
  index->dimension_ = dimension;

  // 使用共享的 StorageManager
  // 注意：新架构下，Join 方法应使用 WindowState 而非 StorageManager 来获取候选
  index->storage_manager_ = storage_;
  storage_->engine_ = std::make_shared<ComputeEngine>();

  const auto blank_controller = std::make_shared<BlankController>(index);

  controller_map_[index->index_id_] = blank_controller;
  index_map_[name] = IdWithType{.id_ = index->index_id_, .index_type_ = index_type};
  return index->index_id_;
}

auto sageFlow::ConcurrencyManager::create_index(const std::string& name, const IndexType& index_type, int dimension,
                                                 const IndexParameters& params) -> int {
  std::shared_ptr<Index> index = nullptr;
  switch (index_type) {
    case IndexType::None:
      return -1;
    case IndexType::IVF:
      if (auto* ivf_params = std::get_if<IVFParameters>(&params)) {
        index = std::make_shared<Ivf>(ivf_params->nlist, ivf_params->rebuild_threshold, ivf_params->nprobes);
      } else {
        // Use default parameters if wrong type provided
        index = std::make_shared<Ivf>();
      }
      break;
    case IndexType::HNSW:
      if (auto* hnsw_params = std::get_if<HNSWParameters>(&params)) {
        index = std::make_shared<HNSW>(hnsw_params->m, hnsw_params->ef_construction, hnsw_params->ef_search);
      } else {
        // Use default parameters if wrong type provided
        index = std::make_shared<HNSW>();
      }
      break;
    case IndexType::Vectraflow:
      index = std::make_shared<VectraFlow>();
      break;
    case IndexType::HDRForest:
      if (auto* hdr_params = std::get_if<HDRForestParameters>(&params)) {
        index = std::make_shared<HDRForest>(hdr_params->n_clusters, hdr_params->f_sections);
      } else {
        index = std::make_shared<HDRForest>();
      }
      break;
    case IndexType::HDRTree:
      index = std::make_shared<HDRTree>(dimension);
      break;
    case IndexType::BruteForce:
    default:
      index = std::make_shared<Knn>();
      break;
  }
  index->index_id_ = index_id_counter_++;
  index->index_type_ = index_type;
  index->dimension_ = dimension;

  // 使用共享的 StorageManager
  index->storage_manager_ = storage_;
  storage_->engine_ = std::make_shared<ComputeEngine>();

  const auto blank_controller = std::make_shared<BlankController>(index);

  controller_map_[index->index_id_] = blank_controller;
  index_map_[name] = IdWithType{.id_ = index->index_id_, .index_type_ = index_type};
  return index->index_id_;
}

auto sageFlow::ConcurrencyManager::create_index(const std::string& name, int dimension) -> int {
  return create_index(name, IndexType::BruteForce, dimension);
}

auto sageFlow::ConcurrencyManager::register_index(const std::string& name, std::shared_ptr<Index> index) -> int {
  if (!index) {
    return -1;
  }
  
  // 分配索引 ID
  index->index_id_ = index_id_counter_++;
  
  // 配置 storage_manager_（遵循索引创建规范）
  index->storage_manager_ = storage_;
  if (storage_ && !storage_->engine_) {
    storage_->engine_ = std::make_shared<ComputeEngine>();
  }
  
  // 创建并发控制器
  const auto blank_controller = std::make_shared<BlankController>(index);
  
  controller_map_[index->index_id_] = blank_controller;
  index_map_[name] = IdWithType{.id_ = index->index_id_, .index_type_ = index->index_type_};
  
  return index->index_id_;
}

auto sageFlow::ConcurrencyManager::drop_index(const std::string& name) -> bool { return false; }

auto sageFlow::ConcurrencyManager::insert(int index_id, std::unique_ptr<VectorRecord> record) -> bool {
  const auto it = controller_map_.find(index_id);
  if (it == controller_map_.end()) {
    return false;
  }
  const auto& controller = it->second;
  return controller->insert(std::move(record));
}

auto sageFlow::ConcurrencyManager::erase(int index_id, std::unique_ptr<VectorRecord> record) -> bool {
  const auto it = controller_map_.find(index_id);
  if (it == controller_map_.end()) {
    return false;
  }
  const auto& controller = it->second;
  return controller->erase(std::move(record));
}

auto sageFlow::ConcurrencyManager::erase(int index_id, uint64_t uid) -> bool {
  const auto it = controller_map_.find(index_id);
  if (it == controller_map_.end()) {
    return false;
  }
  const auto& controller = it->second;
  return controller->erase(uid);
}

auto sageFlow::ConcurrencyManager::query(int index_id, const VectorRecord& record, int k)
    -> std::vector<std::shared_ptr<const VectorRecord>> {
  const auto it = controller_map_.find(index_id);
  if (it == controller_map_.end()) {
    return {};
  }
  const auto& controller = it->second;
  return controller->query(record, k);
}

auto sageFlow::ConcurrencyManager::query_for_join(int index_id, const VectorRecord& record,
                      double join_similarity_threshold) -> std::vector<std::shared_ptr<const VectorRecord>> {
  const auto it = controller_map_.find(index_id);
  if (it == controller_map_.end()) {
    return {};
  }
  const auto& controller = it->second;
  return controller->query_for_join(record, join_similarity_threshold);
}
