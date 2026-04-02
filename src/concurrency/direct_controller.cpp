#include "concurrency/direct_controller.h"
#include "storage/storage_manager.h"
#include "utils/logger.h"

namespace sageFlow {

DirectController::DirectController() = default;

DirectController::DirectController(std::shared_ptr<Index> index)
    : index_(std::move(index)) {
  if (index_ && index_->storage_manager_) {
    storage_manager_ = index_->storage_manager_;
  }
}

DirectController::~DirectController() = default;

auto DirectController::getIndex() const -> std::shared_ptr<Index> {
  return index_;
}

auto DirectController::replaceIndex(std::shared_ptr<Index> new_index) -> bool {
  if (!new_index) return false;
  index_ = std::move(new_index);
  if (index_ && index_->storage_manager_) {
    storage_manager_ = index_->storage_manager_;
  }
  return true;
}

auto DirectController::insert(std::unique_ptr<VectorRecord> record) -> bool {
  if (!record || !storage_manager_) return false;

  const auto uid = record->uid_;
  // Route to the shard owned by this index
  const int shard_id = index_ ? index_->index_id_ : StorageManager::GLOBAL_SHARD;
  storage_manager_->insert(std::move(record), shard_id);

  if (index_) {
    index_->insert(uid);
  }
  return true;
}

auto DirectController::erase(std::unique_ptr<VectorRecord> record) -> bool {
  if (!record) return false;
  return erase(record->uid_);
}

auto DirectController::erase(uint64_t uid) -> bool {
  if (index_) {
    index_->erase(uid);
  }
  const int shard_id = index_ ? index_->index_id_ : StorageManager::GLOBAL_SHARD;
  return storage_manager_ ? storage_manager_->erase(uid, shard_id) : false;
}

auto DirectController::query(const VectorRecord& record, int k)
    -> std::vector<std::shared_ptr<const VectorRecord>> {
  if (!index_ || !storage_manager_) return {};
  const auto uids = index_->query(record, k);
  const int shard_id = index_->index_id_;
  return storage_manager_->getVectorsByUids(uids, shard_id);
}

auto DirectController::query_for_join(const VectorRecord& record,
                                      double join_similarity_threshold,
                                      double similarity_alpha)
    -> std::vector<std::shared_ptr<const VectorRecord>> {
  if (!index_ || !storage_manager_) return {};
  const auto uids = index_->query_for_join(record, join_similarity_threshold, similarity_alpha);
  const int shard_id = index_->index_id_;
  return storage_manager_->getVectorsByUids(uids, shard_id);
}

}  // namespace sageFlow
