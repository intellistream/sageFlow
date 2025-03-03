// data_stream.h
#pragma once

#include <core/common/data_types.h>

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "streaming/data_stream/data_stream.h"
#include "streaming/function/function.h"

namespace candy {

class LogicalPlan {
 public:
  explicit LogicalPlan(std::string name) : name_(std::move(name)) {}

  using FilterFunc = std::function<bool(std::unique_ptr<VectorRecord>&)>;
  using MapFunc = std::function<void(std::unique_ptr<VectorRecord>&)>;
  using JoinFunc = std::function<bool(std::unique_ptr<VectorRecord>&, std::unique_ptr<VectorRecord>&)>;
  using SinkFunc = std::function<void(std::unique_ptr<VectorRecord>&)>;

  // Apply a filter to the stream
  auto filter(FilterFunc& filter_func) -> LogicalPlan*;
  auto filter(FilterFunc filter_func) -> LogicalPlan*;

  // Apply a map function to the stream
  auto map(MapFunc& map_func) -> LogicalPlan*;
  auto map(MapFunc map_func) -> LogicalPlan*;

  // Join with another stream
  auto join(std::unique_ptr<LogicalPlan>& other_plan, JoinFunc& join_func) -> LogicalPlan*;
  auto join(std::unique_ptr<LogicalPlan> other_plan, JoinFunc join_func) -> LogicalPlan*;

  // Write to a sink
  auto writeSink(SinkFunc& sink_func) -> LogicalPlan*;
  auto writeSink(SinkFunc sink_func) -> LogicalPlan*;

  // TODO(pygone): add data source
  auto setDataStream(std::unique_ptr<DataStream>& data_stream) -> void;
  auto setDataStream(std::unique_ptr<DataStream> data_stream) -> void;

  auto GetDataStream() -> std::unique_ptr<DataStream>& { return data_stream_; }

  auto GetTransformations() -> std::vector<std::unique_ptr<Function>>& { return transformations_; }

 private:
  std::string name_;
  std::vector<std::unique_ptr<Function>> transformations_;
  std::unique_ptr<DataStream> data_stream_;
};
}  // namespace candy
