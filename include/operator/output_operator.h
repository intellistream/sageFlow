#pragma once

#include <memory>

#include "operator/operator.h"
#include "stream/data_stream_source/data_stream_source.h"

namespace candy {
enum class OutputChoice { NONE, Broadcast, Hash };  // NOLINT

class OutputOperator final : public Operator {
 public:
  explicit OutputOperator();

  explicit OutputOperator( OutputChoice output_choice, std::shared_ptr<DataStreamSource> stream);

  explicit OutputOperator(std::shared_ptr<DataStreamSource> stream);

  auto open() -> void override;

  auto process(std::unique_ptr<VectorRecord> &data, int slot = 0) -> bool override;

  OutputChoice output_choice_ = OutputChoice::NONE;
  std::shared_ptr<DataStreamSource> stream_ = nullptr;
};
};  // namespace candy
