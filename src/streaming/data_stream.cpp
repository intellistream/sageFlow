#include <streaming/data_stream/data_stream.h>
#include <streaming/function/filter.h>
#include <streaming/function/join.h>
#include <streaming/function/map.h>
#include <streaming/function/sink.h>

#include <memory>

namespace candy {

auto LogicalPlan::filter(FilterFunc &filter_func) -> LogicalPlan * {
  transformations_.emplace_back(std::make_unique<FilterFunction>(name_ + "_filter", filter_func));
  return this;
}

auto LogicalPlan::filter(FilterFunc filter_func) -> LogicalPlan * {
  transformations_.emplace_back(std::make_unique<FilterFunction>(name_ + "_filter", filter_func));
  return this;
}

auto LogicalPlan::map(MapFunc &map_func) -> LogicalPlan * {
  transformations_.emplace_back(std::make_unique<MapFunction>(name_ + "_map", map_func));
  return this;
}

auto LogicalPlan::map(MapFunc map_func) -> LogicalPlan * {
  transformations_.emplace_back(std::make_unique<MapFunction>(name_ + "_map", map_func));
  return this;
}

auto LogicalPlan::join(std::unique_ptr<LogicalPlan> &other_plan, JoinFunc &join_func) -> LogicalPlan * {
  transformations_.emplace_back(std::make_unique<JoinFunction>(name_ + "_join", join_func, other_plan));
  return this;
}

auto LogicalPlan::join(std::unique_ptr<LogicalPlan> other_plan, JoinFunc join_func) -> LogicalPlan * {
  transformations_.emplace_back(std::make_unique<JoinFunction>(name_ + "_join", join_func, other_plan));
  return this;
}

auto LogicalPlan::writeSink(SinkFunc &sink_func) -> LogicalPlan * {
  transformations_.emplace_back(std::make_unique<SinkFunction>(name_ + "_sink", sink_func));
  return this;
}

auto LogicalPlan::writeSink(SinkFunc sink_func) -> LogicalPlan * {
  transformations_.emplace_back(std::make_unique<SinkFunction>(name_ + "_sink", sink_func));
  return this;
}

auto LogicalPlan::setDataStream(std::unique_ptr<DataStream> &data_stream) -> void {
  data_stream_ = std::move(data_stream);
}

auto LogicalPlan::setDataStream(std::unique_ptr<DataStream> data_stream) -> void {
  data_stream_ = std::move(data_stream);
}
}  // namespace candy
