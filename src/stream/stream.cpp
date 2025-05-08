#include "stream/stream.h"

#include "function/function_api.h"

namespace candy {

auto Stream::filter(std::unique_ptr<FilterFunction>& filter_func) -> std::shared_ptr<Stream> {
  return filter(std::move(filter_func));
}

auto Stream::filter(std::unique_ptr<FilterFunction> filter_func) -> std::shared_ptr<Stream> {
  auto stream = std::make_shared<Stream>(name_ + '_' + filter_func->getName());
  stream->function_ = std::move(filter_func);
  streams_.push_back(stream);
  return stream;
}

auto Stream::map(std::unique_ptr<MapFunction>& map_func) -> std::shared_ptr<Stream> { return map(std::move(map_func)); }

auto Stream::map(std::unique_ptr<MapFunction> map_func) -> std::shared_ptr<Stream> {
  auto stream = std::make_shared<Stream>(name_ + '_' + map_func->getName());
  stream->function_ = std::move(map_func);
  streams_.push_back(stream);
  return stream;
}

auto Stream::join(std::shared_ptr<Stream>& other_plan, std::unique_ptr<JoinFunction>& join_func)
    -> std::shared_ptr<Stream> {
  return join(std::move(other_plan), std::move(join_func));
}

auto Stream::join(std::shared_ptr<Stream> other_stream, std::unique_ptr<JoinFunction> join_func)
    -> std::shared_ptr<Stream> {
  auto stream = std::make_shared<Stream>(name_ + "_join_" + other_stream->name_);
  join_func->setOtherStream(std::move(other_stream));
  stream->function_ = std::move(join_func);
  streams_.push_back(stream);
  return stream;
}

auto Stream::window(std::unique_ptr<Function>& window_func) -> std::shared_ptr<Stream> {
  return window(std::move(window_func));
}

auto Stream::window(std::unique_ptr<Function> window_func) -> std::shared_ptr<Stream> {
  auto stream = std::make_shared<Stream>(name_ + '_' + window_func->getName());
  stream->function_ = std::move(window_func);
  streams_.push_back(stream);
  return stream;
}

auto Stream::itopk(std::unique_ptr<Function>& itopk_func) -> std::shared_ptr<Stream> {
  return itopk(std::move(itopk_func));
}

auto Stream::itopk(std::unique_ptr<Function> itopk_func) -> std::shared_ptr<Stream> {
  auto stream = std::make_shared<Stream>(name_ + '_' + itopk_func->getName());
  stream->function_ = std::move(itopk_func);
  streams_.push_back(stream);
  return stream;
}

auto Stream::aggregate(std::unique_ptr<Function>& aggregate_func) -> std::shared_ptr<Stream> {
  return aggregate(std::move(aggregate_func));
}

auto Stream::aggregate(std::unique_ptr<Function> aggregate_func) -> std::shared_ptr<Stream> {
  auto stream = std::make_shared<Stream>(name_ + '_' + aggregate_func->getName());
  stream->function_ = std::move(aggregate_func);
  streams_.push_back(stream);
  return stream;
}

auto Stream::topk(const int32_t index_id, int k) -> std::shared_ptr<Stream> {
  auto stream = std::make_shared<Stream>(name_ + "_topk_" + std::to_string(index_id));
  auto topk_func = std::make_unique<TopkFunction>("topk", k, index_id);
  stream->function_ = std::move(topk_func);
  streams_.push_back(stream);
  return stream;
}

auto Stream::writeSink(std::unique_ptr<SinkFunction>& sink_func) -> std::shared_ptr<Stream> {
  return writeSink(std::move(sink_func));
}

auto Stream::writeSink(std::unique_ptr<SinkFunction> sink_func) -> std::shared_ptr<Stream> {
  auto stream = std::make_shared<Stream>(name_ + '_' + sink_func->getName());
  stream->function_ = std::move(sink_func);
  streams_.push_back(stream);
  return stream;
}
}  // namespace candy