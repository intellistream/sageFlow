#include <runtime/operators/log_operator.h>
#include <iostream>

namespace candy {

void LogOperator::open() {}

void LogOperator::close() {}

void LogOperator::process(const std::shared_ptr<VectorRecord> &record) {
  if (!record || !record->data_) {
  }
  std::cout << "LogOperator: " << record->id_ << std::endl;
}

}  // namespace candy
