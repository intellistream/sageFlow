#include "streaming/stream.h"

#include "runtime/function/filter_function.h"
#include "runtime/function/join_function.h"
#include "runtime/function/map_function.h"
#include "runtime/function/sink_function.h"

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