//
// Created by Pygon on 25-5-1.
//
#include "operator/itopk_operator.h"


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

auto candy::ITopkOperator::process(Response& data, const int slot) -> bool {
  // Process all records in the response
  std::unordered_set<uint64_t> uids;
  
  for (auto& record : data) {
    if (record) {
      auto uid = record->uid_;
      if (uids_.contains(uid)) {
        uids_.erase(uid);
        uids.insert(uid);
      }
    }
  }
  // controller erase
  for (auto& uid : uids_) {
    concurrency_manager_->erase(index_id_, uid);
  }
  uids_.clear();
  
  for (auto& record : data) {
    if (record) {
      auto uid = record->uid_;
      uids_.insert(uid);
      if (!uids.contains(uid)) {
        concurrency_manager_->insert(index_id_, record);
      }
    }
  }
  
  auto topk_record = getRecord();
  auto res = concurrency_manager_->query(index_id_, topk_record, k_);
  
  // Create response from query results
  Response resp;
  for (auto& result_record : res) {
    resp.push_back(std::move(result_record));
  }
  
  emit(0, resp);
  return !data.empty();
}

auto candy::ITopkOperator::getRecord() const -> std::unique_ptr<VectorRecord> {
  return std::make_unique<VectorRecord>(*record_);
}