//
// Created by Pygon on 25-3-12.
//

#ifndef OUTPUT_OPERATOR_H
#define OUTPUT_OPERATOR_H
#include <runtime/operator/base_operator.h>
#include <streaming/data_stream_source/data_stream_source.h>

#include <memory>

namespace candy {
enum class OutputChoice { NONE, Broadcast, Hash };  // NOLINT

class OutputOperator final : public Operator {
 public:
  explicit OutputOperator() : Operator(OperatorType::OUTPUT) {}

  explicit OutputOperator(const OutputChoice output_choice, std::shared_ptr<DataStreamSource> stream)
      : Operator(OperatorType::OUTPUT), output_choice_(output_choice), stream_(std::move(stream)) {}

  explicit OutputOperator(std::shared_ptr<DataStreamSource> stream)
      : Operator(OperatorType::OUTPUT), stream_(std::move(stream)) {}

  auto open() -> void override {
    if (is_open_) {
      return;
    }
    is_open_ = true;
    stream_->Init();
    for (const auto &child : children_) {
      child->open();
    }
    std::unique_ptr<VectorRecord> record = nullptr;
    while ((record = stream_->Next())) {
      process(record);
    }
  }

  auto process(std::unique_ptr<VectorRecord> &data, int slot = 0) -> bool override {
    if (output_choice_ == OutputChoice::Broadcast) {
      for (int i = 0; i < children_.size(); i++) {
        auto copy = std::make_unique<VectorRecord>(*data);
        emit(i, copy);
      }
    } else if (output_choice_ == OutputChoice::Hash) {
      auto id = data->id_.size() % children_.size();
      children_[id]->process(data, 0);
    } else if (output_choice_ == OutputChoice::NONE) {
      emit(0, data);
    }
    return true;
  }

  OutputChoice output_choice_ = OutputChoice::NONE;
  std::shared_ptr<DataStreamSource> stream_ = nullptr;
};
};  // namespace candy
#endif  // OUTPUT_OPERATOR_H
