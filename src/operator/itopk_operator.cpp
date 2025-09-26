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

auto candy::ITopkOperator::process(Response&data, int slot) -> std::optional<Response> {
  // TODO: 多线程改造 - ITopK算子的并发状态管理
  // 在多线程环境中，需要考虑以下改造：
  // 1. uid集合(uids_)的并发访问保护，需要使用线程安全的容器或加锁
  // 2. 索引更新的原子性：insert/erase操作需要保证一致性
  // 3. 增量更新策略：避免频繁的全量重建
  // 4. 分区ITopK：每个线程维护局部状态，定期合并

  if (data.type_ == ResponseType::Record) {
    return std::nullopt;
  }
  if (data.type_ == ResponseType::List) {
    // TODO: 添加互斥锁保护共享状态
    std::lock_guard<std::mutex> lock(state_mutex_);

    const auto& records = data.records_;
    std::unordered_set<uint64_t> uids;
    for (const auto& record : *records) {
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
    for (const auto& record : *records) {
      auto uid = record->uid_;
      uids_.insert(uid);
      auto record_copy = std::make_unique<VectorRecord>(*record);
      concurrency_manager_->insert(index_id_, std::move(record_copy));
    }

    // Perform query and return result
    auto res = concurrency_manager_->query(index_id_, *record_, k_);
    auto result_records = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>();
    result_records->reserve(res.size());
    for (const auto& shared_record : res) {
      result_records->emplace_back(std::make_unique<VectorRecord>(*shared_record));
    }

    return Response{ResponseType::List, std::move(result_records)};
  }
  return std::nullopt;
}

auto candy::ITopkOperator::getRecord() const -> std::unique_ptr<VectorRecord> {
  std::lock_guard<std::mutex> lock(state_mutex_);
  return std::make_unique<VectorRecord>(*record_);
}

auto candy::ITopkOperator::apply(Response&& record, int slot, Collector& collector) -> void {
  if (record.type_ == ResponseType::Record) {
    return; // ITopKOperator通常处理List类型的数据
  }

  if (record.type_ == ResponseType::List && record.records_) {
    std::lock_guard<std::mutex> lock(state_mutex_);

    const auto& records = record.records_;
    std::unordered_set<uint64_t> uids;
    for (const auto& rec : *records) {
      auto uid = rec->uid_;
      if (uids_.contains(uid)) {
        uids_.erase(uid);
        uids.insert(uid);
      }
    }

    // 从索引中删除旧数据
    for (auto& uid : uids_) {
      concurrency_manager_->erase(index_id_, uid);
    }
    uids_.clear();

    // 插入新数据到索引
    for (const auto& rec : *records) {
      auto uid = rec->uid_;
      uids_.insert(uid);
      auto record_copy = std::make_unique<VectorRecord>(*rec);
      concurrency_manager_->insert(index_id_, std::move(record_copy));
    }

    // 执行查询并收集结果
    auto res = concurrency_manager_->query(index_id_, *record_, k_);
    auto result_records = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>();
    result_records->reserve(res.size());
    for (const auto& shared_record : res) {
      result_records->emplace_back(std::make_unique<VectorRecord>(*shared_record));
    }

    Response result{ResponseType::List, std::move(result_records)};
    collector.collect(std::make_unique<Response>(std::move(result)), slot);
  }
}
