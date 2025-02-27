


#include <core/common/data_types.h>
#include <runtime/operators/base_operators.h>
#include <string>

namespace candy {

// Custom operator for logging VectorRecords
class LogOperator : public BaseOperator {
public:
  void open() override;  // Open the operator
  void close() override; // Close the operator
  void process(const std::shared_ptr<VectorRecord> &record) override;
};

} // namespace candy

