//
// Created by Pygon on 25-5-1.
//
#include "operator/itopk_operator.h"

#include <iostream>
#include <mutex>

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

auto candy::ITopkOperator::process(Response& data, int slot) -> bool {
  // TODO: 多线程改造 - ITopK算子的并发状态管理
  // 在多线程环境中，需要考虑以下改造：
  // 1. uid集合(uids_)的并发访问保护，需要使用线程安全的容器或加锁
  // 2. 索引更新的原子性：insert/erase操作需要保证一致性
  // 3. 增量更新策略：避免频繁的全量重建
  // 4. 分区ITopK：每个线程维护局部状态，定期合并

  if (data.type_ == ResponseType::Record) {
    return false;
  }
  if (data.type_ == ResponseType::List) {
    // TODO: 添加互斥锁保护共享状态
    std::lock_guard<std::mutex> lock(state_mutex_);

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