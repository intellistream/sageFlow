// data_stream.h
#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "function/function.h"
#include "common/data_types.h"

namespace candy {
class Function;
class FilterFunction;
class MapFunction;
class JoinFunction;
class SinkFunction;
class WindowFunction;
class Stream {
 public:
  explicit Stream(std::string name) : name_(std::move(name)) {}

  virtual ~Stream() = default;

  // Apply a filter to the stream
  auto filter(std::unique_ptr<FilterFunction> filter_func) -> std::shared_ptr<Stream>;

  // Apply a map function to the stream
  auto map(std::unique_ptr<MapFunction> map_func) -> std::shared_ptr<Stream>;

  // Apply a tumbling window to the stream
  auto tumblingWindow(timestamp_t size) -> std::shared_ptr<Stream>;

  // Apply a sliding window to the stream
  auto slidingWindow(timestamp_t size, timestamp_t slide) -> std::shared_ptr<Stream>;

  // Apply a generic window function to the stream
  auto window(std::unique_ptr<WindowFunction> window_func) -> std::shared_ptr<Stream>;

  // Join with another stream
  auto join(std::shared_ptr<Stream> other_stream, std::unique_ptr<JoinFunction> join_func) -> std::shared_ptr<Stream>;

  // topK
  auto topk(int32_t index_id, int k) -> std::shared_ptr<Stream>;

  // Write to a sink
  auto writeSink(std::unique_ptr<SinkFunction> sink_func) -> std::shared_ptr<Stream>;

  // Set the allowed lateness for late records
  auto allowedLateness(timestamp_t lateness) -> std::shared_ptr<Stream>;

  std::string name_;
  std::unique_ptr<Function> function_ = nullptr;
  std::vector<std::shared_ptr<Stream>> streams_;

  // Stream-specific configuration
  timestamp_t allowed_lateness_ = 0;
};
}  // namespace candy
