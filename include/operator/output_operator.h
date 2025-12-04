#pragma once

#include <memory>

#include "operator/operator.h"
#include "stream/data_stream_source/data_stream_source.h"

namespace sageFlow {
enum class OutputChoice { NONE, Broadcast, Hash };  // NOLINT

class OutputOperator final : public Operator {
 public:
  explicit OutputOperator();

  explicit OutputOperator(OutputChoice output_choice, std::shared_ptr<DataStreamSource> stream);

  explicit OutputOperator(std::shared_ptr<DataStreamSource> stream);

  auto open() -> void override;

  // New method with RuntimeContext support
  auto open(const RuntimeContext& context) -> void override;

  auto process(Response &data, int slot = 0) -> std::optional<Response> override;

  auto apply(Response&& record, int slot, Collector& collector) -> void override;

  // New method with RuntimeContext support
  auto apply(Response&& record, int slot, Collector& collector, 
             const RuntimeContext& context) -> void override;

  auto run(Collector& collector) -> void;

  OutputChoice output_choice_ = OutputChoice::NONE;
  std::shared_ptr<DataStreamSource> stream_ = nullptr;
};
}  // namespace sageFlow