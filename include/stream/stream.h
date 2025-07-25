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
  explicit Stream(std::string name) : name_(std::move(name)), parallelism_(1) {}

  virtual ~Stream() = default;

  // Apply a filter to the stream with parallelism
  auto filter(std::unique_ptr<FilterFunction>& filter_func, size_t parallelism = 1) -> std::shared_ptr<Stream>;
  auto filter(std::unique_ptr<FilterFunction> filter_func, size_t parallelism = 1) -> std::shared_ptr<Stream>;

  // Apply a map function to the stream with parallelism
  auto map(std::unique_ptr<MapFunction>& map_func, size_t parallelism = 1) -> std::shared_ptr<Stream>;
  auto map(std::unique_ptr<MapFunction> map_func, size_t parallelism = 1) -> std::shared_ptr<Stream>;

  // Join with another stream with parallelism
  auto join(std::shared_ptr<Stream>& other_plan, std::unique_ptr<JoinFunction>& join_func, size_t parallelism = 1) -> std::shared_ptr<Stream>;
  auto join(std::shared_ptr<Stream> other_stream, std::unique_ptr<JoinFunction> join_func, size_t parallelism = 1) -> std::shared_ptr<Stream>;

  // Window operations with parallelism
  auto window(std::unique_ptr<Function>& window_func, size_t parallelism = 1) -> std::shared_ptr<Stream>;
  auto window(std::unique_ptr<Function> window_func, size_t parallelism = 1) -> std::shared_ptr<Stream>;

  // ITopK operations with parallelism
  auto itopk(std::unique_ptr<Function>& itopk_func, size_t parallelism = 1) -> std::shared_ptr<Stream>;
  auto itopk(std::unique_ptr<Function> itopk_func, size_t parallelism = 1) -> std::shared_ptr<Stream>;

  // Aggregate operations with parallelism
  auto aggregate(std::unique_ptr<Function>& aggregate_func, size_t parallelism = 1) -> std::shared_ptr<Stream>;
  auto aggregate(std::unique_ptr<Function> aggregate_func, size_t parallelism = 1) -> std::shared_ptr<Stream>;

  // TopK with parallelism
  auto topk(int32_t index_id, int k, size_t parallelism = 1) -> std::shared_ptr<Stream>;

  // Write to a sink with parallelism
  auto writeSink(std::unique_ptr<SinkFunction>& sink_func, size_t parallelism = 1) -> std::shared_ptr<Stream>;
  auto writeSink(std::unique_ptr<SinkFunction> sink_func, size_t parallelism = 1) -> std::shared_ptr<Stream>;

  // Get parallelism of this stream
  auto getParallelism() const -> size_t { return parallelism_; }

  // Set parallelism of this stream
  void setParallelism(size_t parallelism) { parallelism_ = parallelism; }

  std::string name_;
  std::unique_ptr<Function> function_ = nullptr;
  std::vector<std::shared_ptr<Stream>> streams_;

 private:
  size_t parallelism_;
};
}  // namespace candy
