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
#include "index/partitioned_index.h"
#include "utils/logger.h"

#include <shared_mutex>

namespace sageFlow {

ConcurrencyManager::ConcurrencyManager(std::shared_ptr<StorageManager> storage)
    : storage_(std::move(storage)) {}

ConcurrencyManager::~ConcurrencyManager() = default;

auto ConcurrencyManager::create_index(const std::string& name,
                                      const IndexType& index_type,
                                      int dimension) -> int {
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
      index = std::make_shared<HDRTree>(dimension, HDRTree::Config());
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
  if (storage_ && !storage_->engine_) {
    storage_->engine_ = std::make_shared<ComputeEngine>();
  }

  const auto blank_controller = std::make_shared<BlankController>(index);

  {
    std::unique_lock<std::shared_mutex> lk(controller_map_mutex_);
  controller_map_[index->index_id_] = blank_controller;
  }

  index_map_[name] = IdWithType{.id_ = index->index_id_, .index_type_ = index_type};
  return index->index_id_;
}

auto ConcurrencyManager::create_index(const std::string& name,
                                      const IndexType& index_type,
                                      int dimension,
                                                 const IndexParameters& params) -> int {
  std::shared_ptr<Index> index = nullptr;
  switch (index_type) {
    case IndexType::None:
      return -1;
    case IndexType::IVF:
      if (auto* ivf_params = std::get_if<IVFParameters>(&params)) {
        index = std::make_shared<Ivf>(ivf_params->nlist, ivf_params->rebuild_threshold,
                                      ivf_params->nprobes);
      } else {
        index = std::make_shared<Ivf>();
      }
      break;
    case IndexType::HNSW:
      if (auto* hnsw_params = std::get_if<HNSWParameters>(&params)) {
        index = std::make_shared<HNSW>(hnsw_params->m, hnsw_params->ef_construction,
                                       hnsw_params->ef_search);
      } else {
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
      index = std::make_shared<HDRTree>(dimension, HDRTree::Config());
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
  if (storage_ && !storage_->engine_) {
    storage_->engine_ = std::make_shared<ComputeEngine>();
  }

  const auto blank_controller = std::make_shared<BlankController>(index);

  {
    std::unique_lock<std::shared_mutex> lk(controller_map_mutex_);
  controller_map_[index->index_id_] = blank_controller;
  }

  index_map_[name] = IdWithType{.id_ = index->index_id_, .index_type_ = index_type};
  return index->index_id_;
}

auto ConcurrencyManager::create_index(const std::string& name, int dimension) -> int {
  return create_index(name, IndexType::BruteForce, dimension);
}

auto ConcurrencyManager::register_index(const std::string& name, std::shared_ptr<Index> index) -> int {
  if (!index) {
    return -1;
  }
  
  index->index_id_ = index_id_counter_++;
  
  index->storage_manager_ = storage_;
  if (storage_ && !storage_->engine_) {
    storage_->engine_ = std::make_shared<ComputeEngine>();
  }
  
  const auto blank_controller = std::make_shared<BlankController>(index);
  
  {
    std::unique_lock<std::shared_mutex> lk(controller_map_mutex_);
  controller_map_[index->index_id_] = blank_controller;
  }

  index_map_[name] = IdWithType{.id_ = index->index_id_, .index_type_ = index->index_type_};
  
  return index->index_id_;
}

auto ConcurrencyManager::drop_index(const std::string& name) -> bool { return false; }

auto ConcurrencyManager::insert(int index_id, std::unique_ptr<VectorRecord> record) -> bool {
  std::shared_ptr<ConcurrencyController> controller;
  {
    std::shared_lock<std::shared_mutex> lk(controller_map_mutex_);
    const auto it = controller_map_.find(index_id);
    if (it == controller_map_.end()) {
      return false;
    }
    controller = it->second;
  }
  return controller ? controller->insert(std::move(record)) : false;
}

auto ConcurrencyManager::erase(int index_id, std::unique_ptr<VectorRecord> record) -> bool {
  std::shared_ptr<ConcurrencyController> controller;
  {
    std::shared_lock<std::shared_mutex> lk(controller_map_mutex_);
  const auto it = controller_map_.find(index_id);
  if (it == controller_map_.end()) {
    return false;
  }
    controller = it->second;
  }
  return controller ? controller->erase(std::move(record)) : false;
}

auto ConcurrencyManager::erase(int index_id, uint64_t uid) -> bool {
  std::shared_ptr<ConcurrencyController> controller;
  {
    std::shared_lock<std::shared_mutex> lk(controller_map_mutex_);
  const auto it = controller_map_.find(index_id);
  if (it == controller_map_.end()) {
    return false;
  }
    controller = it->second;
  }
  return controller ? controller->erase(uid) : false;
}

auto ConcurrencyManager::query(int index_id, const VectorRecord& record, int k)
    -> std::vector<std::shared_ptr<const VectorRecord>> {
  std::shared_ptr<ConcurrencyController> controller;
  {
    std::shared_lock<std::shared_mutex> lk(controller_map_mutex_);
  const auto it = controller_map_.find(index_id);
  if (it == controller_map_.end()) {
    return {};
  }
    controller = it->second;
  }
  return controller ? controller->query(record, k)
                    : std::vector<std::shared_ptr<const VectorRecord>>{};
}

auto ConcurrencyManager::query_for_join(int index_id, const VectorRecord& record,
                      double join_similarity_threshold,
                                       double similarity_alpha)
    -> std::vector<std::shared_ptr<const VectorRecord>> {
  std::shared_ptr<ConcurrencyController> controller;
  {
    std::shared_lock<std::shared_mutex> lk(controller_map_mutex_);
  const auto it = controller_map_.find(index_id);
  if (it == controller_map_.end()) {
    return {};
  }
    controller = it->second;
  }
  return controller ? controller->query_for_join(record, join_similarity_threshold, similarity_alpha)
                    : std::vector<std::shared_ptr<const VectorRecord>>{};
}

// ==================== 分区索引访问实现 ====================

auto ConcurrencyManager::getPartitionedIndex(int index_id) -> std::shared_ptr<PartitionedIndex> {
  std::shared_ptr<ConcurrencyController> controller;
  {
    std::shared_lock<std::shared_mutex> lk(controller_map_mutex_);
  auto it = controller_map_.find(index_id);
  if (it == controller_map_.end()) {
      return nullptr;
    }
    controller = it->second;
  }

  if (!controller) {
    return nullptr;
  }
  
  auto index = controller->getIndex();
  return std::dynamic_pointer_cast<PartitionedIndex>(index);
}

auto ConcurrencyManager::getPartitionedIndex(int index_id) const
    -> std::shared_ptr<const PartitionedIndex> {
  std::shared_ptr<ConcurrencyController> controller;
  {
    std::shared_lock<std::shared_mutex> lk(controller_map_mutex_);
  auto it = controller_map_.find(index_id);
  if (it == controller_map_.end()) {
      return nullptr;
    }
    controller = it->second;
  }

  if (!controller) {
    return nullptr;
  }
  
  auto index = controller->getIndex();
  return std::dynamic_pointer_cast<const PartitionedIndex>(index);
}

auto ConcurrencyManager::isPartitionedIndex(int index_id) const -> bool {
  return getPartitionedIndex(index_id) != nullptr;
}

auto ConcurrencyManager::getPartitionCount(int index_id) const -> size_t {
  auto partitioned = getPartitionedIndex(index_id);
  if (!partitioned) {
    return 0;
  }
  return partitioned->getNumPartitions();
}

// ==================== 批量构建 + 原子替换（无阻塞查询） ====================

auto ConcurrencyManager::build_index_from_records(const std::string& name,
                                                  const IndexType& index_type,
                                                  int dimension,
                                                  const IndexParameters& params,
                                                  const std::vector<const VectorRecord*>& records) -> int {
  const int new_id = create_index(name, index_type, dimension, params);
  if (new_id < 0) {
    return -1;
  }

  // 批量写入：storage 写一次，索引插入 uid
  for (const auto* r : records) {
    if (!r) {
      continue;
    }
    // 注意：BlankController::insert 会写入 storage 并插入 uid。
    // 这里复用 ConcurrencyManager::insert 保持一致语义。
    auto copy = std::make_unique<VectorRecord>(*r);
    insert(new_id, std::move(copy));
  }

  SAGEFLOW_LOG_INFO("CONCURRENCY_MANAGER", "build_index_from_records done: name={} id={} size={}",
                   name, new_id, records.size());
  return new_id;
}

auto ConcurrencyManager::replace_index_by_id(int old_index_id, int new_index_id) -> bool {
  if (old_index_id < 0 || new_index_id < 0 || old_index_id == new_index_id) {
    return false;
  }

  std::shared_ptr<ConcurrencyController> old_controller;
  std::shared_ptr<ConcurrencyController> new_controller;
  {
    std::shared_lock<std::shared_mutex> lk(controller_map_mutex_);
    auto it_old = controller_map_.find(old_index_id);
    auto it_new = controller_map_.find(new_index_id);
    if (it_old == controller_map_.end() || it_new == controller_map_.end()) {
      return false;
    }
    old_controller = it_old->second;
    new_controller = it_new->second;
  }

  auto new_index = new_controller ? new_controller->getIndex() : nullptr;
  if (!old_controller || !new_index) {
    return false;
  }

  // 1) 先开启双写：保证切换窗口内增量不会丢
  old_controller->enableDoubleWrite(true, new_index);

  // 2) 原子替换主索引
  if (!old_controller->replaceIndex(new_index)) {
    old_controller->enableDoubleWrite(false, nullptr);
    return false;
  }

  // 3) 清理 new_index_id 的路由：避免外部继续使用 new_id（此处只删除 controller_map_，不 drop storage）
  {
    std::unique_lock<std::shared_mutex> lk(controller_map_mutex_);
    controller_map_.erase(new_index_id);
  }

  // 4) 修正 index_map_（如果有名字指向 new_id，则改为 old_id）
  for (auto& [name, id_with_type] : index_map_) {
    if (id_with_type.id_ == new_index_id) {
      id_with_type.id_ = old_index_id;
    }
  }

  SAGEFLOW_LOG_INFO("CONCURRENCY_MANAGER", "replace_index_by_id done: old_id={} new_id={}",
                   old_index_id, new_index_id);
  return true;
}

}  // namespace sageFlow
