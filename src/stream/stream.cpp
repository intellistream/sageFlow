#include "stream/stream.h"

#include "function/function_api.h"

namespace candy {

auto Stream::filter(std::unique_ptr<FilterFunction>& filter_func, size_t parallelism) -> std::shared_ptr<Stream> {
  return filter(std::move(filter_func), parallelism);
}

auto Stream::filter(std::unique_ptr<FilterFunction> filter_func, size_t parallelism) -> std::shared_ptr<Stream> {
  auto stream = std::make_shared<Stream>(name_ + '_' + filter_func->getName());
  stream->function_ = std::move(filter_func);
  stream->setParallelism(parallelism);
  streams_.push_back(stream);
  return stream;
}

auto Stream::map(std::unique_ptr<MapFunction>& map_func, size_t parallelism) -> std::shared_ptr<Stream> {
  return map(std::move(map_func), parallelism);
}

auto Stream::map(std::unique_ptr<MapFunction> map_func, size_t parallelism) -> std::shared_ptr<Stream> {
  auto stream = std::make_shared<Stream>(name_ + '_' + map_func->getName());
  stream->function_ = std::move(map_func);
  stream->setParallelism(parallelism);
  streams_.push_back(stream);
  return stream;
}

auto Stream::join(std::shared_ptr<Stream>& other_plan, std::unique_ptr<JoinFunction>& join_func, size_t parallelism)
    -> std::shared_ptr<Stream> {
  return join(std::move(other_plan), std::move(join_func), /*method*/ "bruteforce_lazy", /*threshold*/ 0.8, parallelism);
}

auto Stream::join(std::shared_ptr<Stream> other_stream, std::unique_ptr<JoinFunction> join_func, size_t parallelism)
    -> std::shared_ptr<Stream> {
  // 退化到有默认配置的重载
  return join(std::move(other_stream), std::move(join_func), /*method*/ "bruteforce_lazy", /*threshold*/ 0.8, parallelism);
}

auto Stream::join(std::shared_ptr<Stream>& other_plan,
                  std::unique_ptr<JoinFunction>& join_func,
                  const std::string& join_method,
                  double similarity_threshold,
                  size_t parallelism) -> std::shared_ptr<Stream> {
  return join(std::move(other_plan), std::move(join_func), join_method, similarity_threshold, parallelism);
}

auto Stream::join(std::shared_ptr<Stream> other_stream,
                  std::unique_ptr<JoinFunction> join_func,
                  const std::string& join_method,
                  double similarity_threshold,
                  size_t parallelism) -> std::shared_ptr<Stream> {
  auto stream = std::make_shared<Stream>(name_ + "_join_" + other_stream->name_);
  join_func->setOtherStream(std::move(other_stream));
  stream->function_ = std::move(join_func);
  stream->setParallelism(parallelism);
  // 将 Join 配置存放到子 Stream 上，Planner 建图时直接读取
  stream->setJoinMethod(join_method);
  stream->setJoinSimilarityThreshold(similarity_threshold);
  streams_.push_back(stream);
  return stream;
}

auto Stream::window(std::unique_ptr<Function>& window_func, size_t parallelism) -> std::shared_ptr<Stream> {
  return window(std::move(window_func), parallelism);
}

auto Stream::window(std::unique_ptr<Function> window_func, size_t parallelism) -> std::shared_ptr<Stream> {
  auto stream = std::make_shared<Stream>(name_ + '_' + window_func->getName());
  stream->function_ = std::move(window_func);
  stream->setParallelism(parallelism);
  streams_.push_back(stream);
  return stream;
}

auto Stream::itopk(std::unique_ptr<Function>& itopk_func, size_t parallelism) -> std::shared_ptr<Stream> {
  return itopk(std::move(itopk_func), parallelism);
}

auto Stream::itopk(std::unique_ptr<Function> itopk_func, size_t parallelism) -> std::shared_ptr<Stream> {
  auto stream = std::make_shared<Stream>(name_ + '_' + itopk_func->getName());
  stream->function_ = std::move(itopk_func);
  stream->setParallelism(parallelism);
  streams_.push_back(stream);
  return stream;
}

auto Stream::aggregate(std::unique_ptr<Function>& aggregate_func, size_t parallelism) -> std::shared_ptr<Stream> {
  return aggregate(std::move(aggregate_func), parallelism);
}

auto Stream::aggregate(std::unique_ptr<Function> aggregate_func, size_t parallelism) -> std::shared_ptr<Stream> {
  auto stream = std::make_shared<Stream>(name_ + '_' + aggregate_func->getName());
  stream->function_ = std::move(aggregate_func);
  stream->setParallelism(parallelism);
  streams_.push_back(stream);
  return stream;
}

auto Stream::topk(const int32_t index_id, int k, size_t parallelism) -> std::shared_ptr<Stream> {
  auto stream = std::make_shared<Stream>(name_ + "_topk_" + std::to_string(index_id));
  auto topk_func = std::make_unique<TopkFunction>("topk", k, index_id);
  stream->function_ = std::move(topk_func);
  stream->setParallelism(parallelism);
  streams_.push_back(stream);
  return stream;
}

auto Stream::writeSink(std::unique_ptr<SinkFunction>& sink_func, size_t parallelism) -> std::shared_ptr<Stream> {
  return writeSink(std::move(sink_func), parallelism);
}

auto Stream::writeSink(std::unique_ptr<SinkFunction> sink_func, size_t parallelism) -> std::shared_ptr<Stream> {
  auto stream = std::make_shared<Stream>(name_ + '_' + sink_func->getName());
  stream->function_ = std::move(sink_func);
  stream->setParallelism(parallelism);
  streams_.push_back(stream);
  return stream;
}
}  // namespace candy