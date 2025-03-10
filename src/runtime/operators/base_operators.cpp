#include <runtime/operator/base_operator.h>

#include <iostream>  // For logging and debugging

namespace candy {

auto Operator::process(std::unique_ptr<VectorRecord>& record) -> bool {
  // Default implementation: Simply emit the record to the next operator
  std::cout << "[BaseOperator] Processing record ID: " << record->id_ << '\n';
  emit(0,record);
  return true;
}

}  // namespace candy