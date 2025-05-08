#pragma once

#include <memory>
#include <string>

#include "common/data_types.h"
#include "operator/operator.h"
#include "stream/data_stream_source/data_stream_source.h"

namespace candy {

enum class OutputChoice {
  STDOUT,
  FILE,
};

class OutputOperator final : public Operator {
 public:
  OutputOperator();
  explicit OutputOperator(const OutputChoice output_choice, std::shared_ptr<DataStreamSource> stream);
  explicit OutputOperator(std::shared_ptr<DataStreamSource> stream);

  auto process(DataElement &element, int slot = 0) -> bool override;
  void open() override;

 private:
  // Helper methods for managing data elements with move semantics
  DataElement createMovedElement(const DataElement &original);
  std::unique_ptr<VectorRecord> cloneVectorRecord(const VectorRecord &record);

  OutputChoice output_choice_ = OutputChoice::STDOUT;  // Default output to stdout
  std::shared_ptr<DataStreamSource> stream_ = nullptr;
};

}  // namespace candy
