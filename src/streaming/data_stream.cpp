#include "streaming/data_stream/data_stream.h"

#include <streaming/logical_plan.h>

#include <memory>

#include "runtime/function/filter_function.h"
#include "runtime/function/join_function.h"
#include "runtime/function/map_function.h"
#include "runtime/function/sink_function.h"

namespace candy {

auto LogicalPlan::filter(std::unique_ptr<FilterFunction> &filter_func) -> LogicalPlan * {
  functions_.emplace_back(std::move(filter_func));
  return this;
}

auto LogicalPlan::filter(std::unique_ptr<FilterFunction> filter_func) -> LogicalPlan * {
  functions_.emplace_back(std::move(filter_func));
  return this;
}

auto LogicalPlan::map(std::unique_ptr<MapFunction> &map_func) -> LogicalPlan * {
  functions_.emplace_back(std::move(map_func));
  return this;
}

auto LogicalPlan::map(std::unique_ptr<MapFunction> map_func) -> LogicalPlan * {
  functions_.emplace_back(std::move(map_func));
  return this;
}

auto LogicalPlan::join(std::unique_ptr<LogicalPlan> &other_plan,
                       std::unique_ptr<JoinFunction> &join_func) -> LogicalPlan * {
  join_func->setOtherPlan(std ::move(other_plan));
  functions_.emplace_back(std::move(join_func));
  return this;
}

auto LogicalPlan::join(std::unique_ptr<LogicalPlan> other_plan,
                       std::unique_ptr<JoinFunction> join_func) -> LogicalPlan * {
  join_func->setOtherPlan(std::move(other_plan));
  functions_.emplace_back(std::move(join_func));
  return this;
}

auto LogicalPlan::writeSink(std::unique_ptr<SinkFunction> &sink_func) -> LogicalPlan * {
  functions_.emplace_back(std::move(sink_func));
  return this;
}

auto LogicalPlan::writeSink(std::unique_ptr<SinkFunction> sink_func) -> LogicalPlan * {
  functions_.emplace_back(std::move(sink_func));
  return this;
}

auto LogicalPlan::setDataStream(std::unique_ptr<DataStream> &data_stream) -> void {
  data_stream_ = std::move(data_stream);
}

auto LogicalPlan::setDataStream(std::unique_ptr<DataStream> data_stream) -> void {
  data_stream_ = std::move(data_stream);
}
}  // namespace candy
