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
  storage_manager_->insert(std::move(record));
  // gpu insert
  return index_->insert(uid);
}

auto sageFlow::BlankController::erase(std::unique_ptr<VectorRecord> record) -> bool { return true; }

auto sageFlow::BlankController::erase(const uint64_t uid) -> bool {
  if (index_) {
    index_->erase(uid);
  }
  return storage_manager_->erase(uid);
}

auto sageFlow::BlankController::query(const VectorRecord& record, int k)
    -> std::vector<std::shared_ptr<const VectorRecord>> {
  const auto uids = index_->query(record, k);
  return storage_manager_->getVectorsByUids(uids);
}

auto sageFlow::BlankController::query_for_join(const VectorRecord& record,
                                            double join_similarity_threshold,
                                            double similarity_alpha) -> std::vector<std::shared_ptr<const VectorRecord>> {
  const auto uids  = index_->query_for_join(record, join_similarity_threshold, similarity_alpha);
  return storage_manager_->getVectorsByUids(uids);
}
