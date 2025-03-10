// data_stream.h
#pragma once

#include <core/common/data_types.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "streaming/data_stream/data_stream.h"

namespace candy {
class Function;
class FilterFunction;
class MapFunction;
class JoinFunction;
class SinkFunction;

class LogicalPlan {
 public:
  explicit LogicalPlan(std::string name) : name_(std::move(name)) {}

  // Apply a filter to the stream
  auto filter(std::unique_ptr<FilterFunction>& filter_func) -> LogicalPlan*;
  auto filter(std::unique_ptr<FilterFunction> filter_func) -> LogicalPlan*;

  // Apply a map function to the stream
  auto map(std::unique_ptr<MapFunction>& map_func) -> LogicalPlan*;
  auto map(std::unique_ptr<MapFunction> map_func) -> LogicalPlan*;

  // Join with another stream
  auto join(std::unique_ptr<LogicalPlan>& other_plan, std::unique_ptr<JoinFunction>& join_func) -> LogicalPlan*;
  auto join(std::unique_ptr<LogicalPlan> other_plan, std::unique_ptr<JoinFunction> join_func) -> LogicalPlan*;

  // Write to a sink
  auto writeSink(std::unique_ptr<SinkFunction>& sink_func) -> LogicalPlan*;
  auto writeSink(std::unique_ptr<SinkFunction> sink_func) -> LogicalPlan*;

  // TODO(pygone): add data source
  auto setDataStream(std::unique_ptr<DataStream>& data_stream) -> void;
  auto setDataStream(std::unique_ptr<DataStream> data_stream) -> void;

  auto GetDataStream() -> std::unique_ptr<DataStream>& { return data_stream_; }

  auto GetFunctions() -> std::vector<std::unique_ptr<Function>>& { return functions_; }

 private:
  std::string name_;
  std::vector<std::unique_ptr<Function>> functions_;
  std::unique_ptr<DataStream> data_stream_;
};
}  // namespace candy
