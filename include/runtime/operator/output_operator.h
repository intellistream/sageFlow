#pragma once

#include <runtime/operator/operator.h>
#include <streaming/data_stream_source/data_stream_source.h>

#include <memory>

namespace candy {
enum class OutputChoice { NONE, Broadcast, Hash };  // NOLINT

class OutputOperator final : public Operator {
 public:
  explicit OutputOperator();

  explicit OutputOperator(const OutputChoice output_choice, std::shared_ptr<DataStreamSource> stream);

  explicit OutputOperator(std::shared_ptr<DataStreamSource> stream);

  auto open() -> void override;

  auto process(std::unique_ptr<VectorRecord> &data, int slot = 0) -> bool override;

  OutputChoice output_choice_ = OutputChoice::NONE;
  std::shared_ptr<DataStreamSource> stream_ = nullptr;
};
};  // namespace candy
