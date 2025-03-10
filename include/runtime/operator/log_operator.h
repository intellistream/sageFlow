


#include <core/common/data_types.h>
#include <runtime/operator/base_operator.h>
#include <string>

namespace candy {

// Custom operator for logging VectorRecords
class LogOperator final : public Operator {
public:
  void open() override;  // Open the operator
  void close() override; // Close the operator
  auto process(std::unique_ptr<VectorRecord> &record) -> bool override;
};

} // namespace candy

