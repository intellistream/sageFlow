//
// Created by Pygon on 25-5-7.
//
#include "operator/aggregate_operator.h"

#include <mutex>
#include "function/aggregate_function.h"

candy::AggregateOperator::AggregateOperator(std::unique_ptr<Function>& aggregate_func)
    : Operator(OperatorType::AGGREGATE), aggregate_func_(std::move(aggregate_func)) {}

auto Sum(std::unique_ptr<candy::VectorRecord>& record, std::unique_ptr<candy::VectorRecord>& record2) -> void {
  const auto& data = record->data_;
  const auto& data2 = record2->data_;
  if (data.type_ == candy::DataType::Float32) {
    const auto d1 = reinterpret_cast<float*>(data.data_.get());
    const auto d2 = reinterpret_cast<float*>(data2.data_.get());
    for (int i = 0; i < data.dim_; ++i) {
      d1[i] += d2[i];
    }
  }
}

void Avg(const std::unique_ptr<candy::VectorRecord>& record, int size) {
  if (const auto& data = record->data_; data.type_ == candy::DataType::Float32) {
    const auto d1 = reinterpret_cast<float*>(data.data_.get());
    for (int i = 0; i < data.dim_; ++i) {
      d1[i] /= size;
    }
  }
}

auto candy::AggregateOperator::process(Response& data, int slot) -> bool {
  // TODO: 多线程改造 - 当前实现为单线程版本
  // 在多线程环境中，需要考虑以下改造：
  // 1. 使用线程安全的累加器或状态管理
  // 2. 考虑分区聚合：每个线程维护局部状态，最后合并
  // 3. 使用原子操作或锁保护共享状态
  // 4. 实现分布式聚合模式（Map-Reduce风格）

  const auto aggregate_func = dynamic_cast<AggregateFunction*>(aggregate_func_.get());
  if (data.type_ == ResponseType::List) {
    const auto records = std::move(data.records_);
    const auto aggregate_type = aggregate_func->getAggregateType();
    auto begin = records->begin();
    auto record = std::move(*begin);
    auto records_size = records->size();
    if (aggregate_type == AggregateType::Avg) {
      for (auto it = begin + 1; it != records->end(); ++it) {
        Sum(record, *it);
      }
      Avg(record, records_size);
    }
    auto resp = Response{ResponseType::Record, std::move(record)};
    emit(0, resp);
    return true;
  }

  return false;
}