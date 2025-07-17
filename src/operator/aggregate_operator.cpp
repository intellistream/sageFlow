//
// Created by Pygon on 25-5-7.
//
#include "operator/aggregate_operator.h"

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

auto candy::AggregateOperator::process(Response& data, const int slot) -> bool {
  const auto aggregate_func = dynamic_cast<AggregateFunction*>(aggregate_func_.get());
  
  if (!data.empty()) {
    const auto aggregate_type = aggregate_func->getAggregateType();
    auto record = std::move(data[0]); // Take first record as base
    auto records_size = data.size();
    
    if (aggregate_type == AggregateType::Avg) {
      // Aggregate all remaining records
      for (size_t i = 1; i < data.size(); ++i) {
        if (data[i]) {
          Sum(record, data[i]);
        }
      }
      Avg(record, records_size);
    }
    
    Response resp;
    resp.push_back(std::move(record));
    emit(0, resp);
    return true;
  }
  
  return false;
}