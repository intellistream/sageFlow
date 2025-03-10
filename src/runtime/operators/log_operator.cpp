#include <runtime/operator/log_operator.h>

#include <iostream>

namespace candy {

void LogOperator::open() {}

void LogOperator::close() {}

auto LogOperator::process(std::unique_ptr<VectorRecord> &record) -> bool {
  if (!record || !record->data_) {
  }
  std::cout << "LogOperator: " << record->id_ << std::endl;
  return true;
}

}  // namespace candy
