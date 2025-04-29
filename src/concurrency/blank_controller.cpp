//
// Created by Pygon on 25-4-18.
//
#include "concurrency/blank_controller.h"

candy::BlankController::BlankController() = default;

candy::BlankController::BlankController(std::shared_ptr<Index> index) {
  index_ = std::move(index);
  storage_manager_ = index_->storage_manager_;
  if (index_->index_type_ == IndexType::None) {
    index_ = nullptr;
  }
}

candy::BlankController::~BlankController() = default;

auto candy::BlankController::insert(std::unique_ptr<VectorRecord>& record) -> bool {
  auto uid = record->uid_;
  storage_manager_->insert(record);
  // Pay attention to "Record is empty now "!
  index_->insert(uid);
  return true;
}

auto candy::BlankController::erase(std::unique_ptr<VectorRecord>& record) -> bool { return true; }

auto candy::BlankController::query(std::unique_ptr<VectorRecord>& record, int k)
    -> std::vector<std::unique_ptr<candy::VectorRecord>> {
  const auto idxes = index_->query(record, k);
  return storage_manager_->getVectorsByUids(idxes);
}