//
// Created by Pygon on 25-5-1.
//
#include "operator/itopk_operator.h"

#include <iostream>

#include "function/itopk_function.h"

candy::ITopkOperator::ITopkOperator(std::unique_ptr<Function>& func,
                                    const std::shared_ptr<ConcurrencyManager>& concurrency_manager)
    : Operator(OperatorType::ITOPK), itopk_func_(std::move(func)), concurrency_manager_(concurrency_manager) {
  auto itopk_func = dynamic_cast<ITopkFunction*>(itopk_func_.get());
  if (itopk_func != nullptr) {
    k_ = itopk_func->getK();
  }
  index_id_ = concurrency_manager_->create_index("itopk_index", IndexType::BruteForce, itopk_func->getDim());
  record_ = itopk_func->getRecord();
}

static int cnt = 0;

auto candy::ITopkOperator::process(Response& data, const int slot) -> bool {
  // std::cout << "ITopkOperator: " << ++cnt << '\n';
  if (data.type_ == ResponseType::Record) {
    return false;
  }
  if (data.type_ == ResponseType::List) {
    const auto records = std::move(data.records_);
    std::unordered_set<uint64_t> uids;
    for (auto& record : *records) {
      auto uid = record->uid_;
      if (uids_.contains(uid)) {
        uids_.erase(uid);
        uids.insert(uid);
      }
    }
    // controller erase
    for (auto& uid : uids_) {
      concurrency_manager_->erase(index_id_, uid);
    }
    uids_.clear();
    for (auto& record : *records) {
      auto uid = record->uid_;
      uids_.insert(uid);
      if (!uids.contains(uid)) {
        concurrency_manager_->insert(index_id_, record);
      }
    }
    auto topk_record = getRecord();
    auto res = concurrency_manager_->query(index_id_, topk_record, k_);
    auto topk_records = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>(std::move(res));
    auto resp = Response{ResponseType::List, std::move(topk_records)};
    emit(0, resp);
    return true;
  }
  return false;
}

auto candy::ITopkOperator::getRecord() const -> std::unique_ptr<VectorRecord> {
  return std::make_unique<VectorRecord>(*record_);
}