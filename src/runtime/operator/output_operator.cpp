#include "runtime/operator/output_operator.h"

candy::OutputOperator::OutputOperator() : Operator(OperatorType::OUTPUT) {}

candy::OutputOperator::OutputOperator(const OutputChoice output_choice, std::shared_ptr<DataStreamSource> stream)
    : Operator(OperatorType::OUTPUT), output_choice_(output_choice), stream_(std::move(stream)) {}

candy::OutputOperator::OutputOperator(std::shared_ptr<DataStreamSource> stream)
    : Operator(OperatorType::OUTPUT), stream_(std::move(stream)) {}

void candy::OutputOperator::open() {
  if (is_open_) {
    return;
  }
  is_open_ = true;
  stream_->Init();
  for (const auto& child : children_) {
    child->open();
  }
  std::unique_ptr<VectorRecord> record = nullptr;
  while ((record = stream_->Next())) {
    process(record);
  }
}

bool candy::OutputOperator::process(std::unique_ptr<VectorRecord>& data, int slot) {
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