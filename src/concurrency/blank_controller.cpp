//
// Created by Pygon on 25-4-18.
//
#include "concurrency/blank_controller.h"

#include "utils/logger.h"

namespace sageFlow {

BlankController::BlankController() = default;

BlankController::BlankController(std::shared_ptr<Index> index) {
  {
    std::unique_lock<std::shared_mutex> lk(index_mutex_);
  index_ = std::move(index);
    if (index_ && index_->index_type_ == IndexType::None) {
      index_.reset();
    }
  }

  if (index_ && index_->storage_manager_) {
  storage_manager_ = index_->storage_manager_;
  }
}

BlankController::~BlankController() = default;

auto BlankController::getIndex() const -> std::shared_ptr<Index> {
  std::shared_lock<std::shared_mutex> lk(index_mutex_);
  return index_;
}

auto BlankController::enableDoubleWrite(bool enable, std::shared_ptr<Index> shadow) -> void {
  std::unique_lock<std::shared_mutex> lk(index_mutex_);
  if (enable) {
    shadow_index_ = std::move(shadow);
    double_write_enabled_ = true;
  } else {
    double_write_enabled_ = false;
    shadow_index_.reset();
  }
}

auto BlankController::replaceIndex(std::shared_ptr<Index> new_index) -> bool {
  if (!new_index) {
    return false;
  }

  {
    std::unique_lock<std::shared_mutex> lk(index_mutex_);
    index_ = std::move(new_index);
  }

  // storage_manager_ 仍然是 ConcurrencyManager 全局共享的 StorageManager
  if (index_ && index_->storage_manager_) {
    storage_manager_ = index_->storage_manager_;
  }

  return true;
}

auto BlankController::insert(std::unique_ptr<VectorRecord> record) -> bool {
  if (!record) {
    return false;
  }
  RecordView shared_record = std::move(record);
  return insert(std::move(shared_record));
}

auto BlankController::insert(RecordView record) -> bool {
  if (!record) {
    return false;
  }

  const auto uid = record->uid_;

  // 1) 写 storage（只写一次）
  if (!storage_manager_) {
    return false;
  }
  storage_manager_->insert(record);

  // 2) 获取当前索引快照（在锁内复制 shared_ptr，然后解锁进行 insert）
  std::shared_ptr<Index> idx;
  std::shared_ptr<Index> shadow;
  bool double_write = false;
  {
    std::shared_lock<std::shared_mutex> lk(index_mutex_);
    idx = index_;
    double_write = double_write_enabled_;
    if (double_write) {
      shadow = shadow_index_;
    }
  }

  bool ok = true;
  if (idx) {
    ok = idx->insert(uid);
}

  // 3) 双写 shadow
  if (double_write && shadow) {
    shadow->insert(uid);
  }

  return ok;
}

auto BlankController::erase(std::unique_ptr<VectorRecord> record) -> bool {
  if (!record) {
    return false;
  }
  return erase(record->uid_);
}

auto BlankController::erase(const uint64_t uid) -> bool {
  std::shared_ptr<Index> idx;
  std::shared_ptr<Index> shadow;
  bool double_write = false;
  {
    std::shared_lock<std::shared_mutex> lk(index_mutex_);
    idx = index_;
    double_write = double_write_enabled_;
    if (double_write) {
      shadow = shadow_index_;
    }
  }

  if (idx) {
    idx->erase(uid);
  }
  if (double_write && shadow) {
    shadow->erase(uid);
  }

  return storage_manager_ ? storage_manager_->erase(uid) : false;
}

auto BlankController::query(const VectorRecord& record, int k)
    -> std::vector<std::shared_ptr<const VectorRecord>> {
  std::shared_ptr<Index> idx;
  {
    std::shared_lock<std::shared_mutex> lk(index_mutex_);
    idx = index_;
  }

  if (!idx || !storage_manager_) {
    return {};
  }

  const auto uids = idx->query(record, k);
  return storage_manager_->getVectorsByUids(uids);
}

auto BlankController::query_for_join(const VectorRecord& record,
                                            double join_similarity_threshold,
                                    double similarity_alpha)
    -> std::vector<std::shared_ptr<const VectorRecord>> {
  std::shared_ptr<Index> idx;
  {
    std::shared_lock<std::shared_mutex> lk(index_mutex_);
    idx = index_;
  }

  if (!idx || !storage_manager_) {
    return {};
  }

  const auto uids = idx->query_for_join(record, join_similarity_threshold, similarity_alpha);
  return storage_manager_->getVectorsByUids(uids);
}

}  // namespace sageFlow
