// data_stream.h
#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "function/function.h"

namespace candy {
class Function;
class FilterFunction;
class MapFunction;
class JoinFunction;
class SinkFunction;

class Stream {
 public:
  explicit Stream(std::string name) : name_(std::move(name)) {}

  virtual ~Stream() = default;

  // Apply a filter to the stream
  auto filter(std::unique_ptr<FilterFunction>& filter_func) -> std::shared_ptr<Stream>;

  auto filter(std::unique_ptr<FilterFunction> filter_func) -> std::shared_ptr<Stream>;

  // Apply a map function to the stream
  auto map(std::unique_ptr<MapFunction>& map_func) -> std::shared_ptr<Stream>;
  auto map(std::unique_ptr<MapFunction> map_func) -> std::shared_ptr<Stream>;

  // Join with another stream
  auto join(std::shared_ptr<Stream>& other_plan, std::unique_ptr<JoinFunction>& join_func) -> std::shared_ptr<Stream>;
  auto join(std::shared_ptr<Stream> other_stream, std::unique_ptr<JoinFunction> join_func) -> std::shared_ptr<Stream>;

  // Write to a sink
  auto writeSink(std::unique_ptr<SinkFunction>& sink_func) -> std::shared_ptr<Stream>;
  auto writeSink(std::unique_ptr<SinkFunction> sink_func) -> std::shared_ptr<Stream>;

  std::string name_;
  std::unique_ptr<Function> function_ = nullptr;

  std::vector<std::shared_ptr<Stream>> streams_;
};
}  // namespace candy
