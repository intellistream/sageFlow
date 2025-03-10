#include <runtime/operator/base_operator.h>

#include <iostream>  // For logging and debugging

namespace candy {

bool Operator::process(std::unique_ptr<VectorRecord>& record) {
  // Default implementation: Simply emit the record to the next operator
  std::cout << "[BaseOperator] Processing record ID: " << record->id_ << '\n';
  emit(record);
  return true;
}

}  // namespace candy