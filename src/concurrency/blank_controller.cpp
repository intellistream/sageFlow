//
// Created by Pygon on 25-4-18.
//
#include "concurrency/blank_controller.h"

sageFlow::BlankController::BlankController() = default;

sageFlow::BlankController::BlankController(std::shared_ptr<Index> index) {
  index_ = std::move(index);
  storage_manager_ = index_->storage_manager_;
  if (index_->index_type_ == IndexType::None) {
    index_ = nullptr;
  }
}

sageFlow::BlankController::~BlankController() = default;

auto sageFlow::BlankController::insert(std::unique_ptr<VectorRecord> record) -> bool {
  if (!record) {
    return false;
  }
  const auto uid = record->uid_;
  {
    std::unique_lock lock(local_uids_mutex_);
    local_uids_.insert(uid);
  }
  storage_manager_->insert(std::move(record));
  // gpu insert
  if (index_) {
    return index_->insert(uid);
  }
  return true;
}

auto sageFlow::BlankController::erase(std::unique_ptr<VectorRecord> record) -> bool { return true; }

auto sageFlow::BlankController::erase(const uint64_t uid) -> bool {
  if (index_) {
    index_->erase(uid);
  }
  {
    std::unique_lock lock(local_uids_mutex_);
    local_uids_.erase(uid);
  }
  return storage_manager_->erase(uid);
}

auto sageFlow::BlankController::query(const VectorRecord& record, int k)
    -> std::vector<std::shared_ptr<const VectorRecord>> {
  const auto uids = index_->query(record, k);
  std::vector<uint64_t> local;
  local.reserve(uids.size());
  {
    std::shared_lock lock(local_uids_mutex_);
    for (auto uid : uids) {
      if (local_uids_.contains(uid)) {
        local.push_back(uid);
      }
    }
  }
  return storage_manager_->getVectorsByUids(local);
}

auto sageFlow::BlankController::query_for_join(const VectorRecord& record,
                                            double join_similarity_threshold,
                                            double similarity_alpha) -> std::vector<std::shared_ptr<const VectorRecord>> {
  const auto uids  = index_->query_for_join(record, join_similarity_threshold, similarity_alpha);
  std::vector<uint64_t> local;
  local.reserve(uids.size());
  {
    std::shared_lock lock(local_uids_mutex_);
    for (auto uid : uids) {
      if (local_uids_.contains(uid)) {
        local.push_back(uid);
      }
    }
  }
  return storage_manager_->getVectorsByUids(local);
}
