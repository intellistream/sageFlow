#include <stream/stream_environment.h>

#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

namespace candy {

auto StreamEnvironment::loadConfiguration(const std::string &file_path) -> ConfigMap {
  ConfigMap config;
  if (!config.fromFile(file_path)) {
    throw std::runtime_error("Failed to load configuration from: " + file_path);
  }
  return config;
}

// 计划在这里进行多线程的分配
auto StreamEnvironment::execute() -> void {
  if (streams_.empty()) {
    throw std::runtime_error("No streams to execute.");
  }

  if (is_running_) {
    std::cout << "StreamEnvironment is already running." << std::endl;
    return;
  }

  std::cout << "Building execution graph from " << streams_.size() << " streams..." << std::endl;

  // 使用Planner构建执行图而不是单独的算子
  for (auto &stream : streams_) {
    planner_->planToExecutionGraph(stream, execution_graph_.get(), default_parallelism_);
  }

  // 构建执行图
  execution_graph_->buildGraph();

  // 启动多线程执行
  execution_graph_->start();
  is_running_ = true;

  std::cout << "StreamEnvironment started successfully." << std::endl;
}

auto StreamEnvironment::stop() -> void {
  if (!is_running_) {
    return;
  }

  std::cout << "Stopping StreamEnvironment..." << std::endl;
  execution_graph_->stop();
  is_running_ = false;
}

auto StreamEnvironment::awaitTermination() -> void {
  if (!is_running_) {
    return;
  }

  execution_graph_->join();
  is_running_ = false;
  std::cout << "StreamEnvironment terminated." << std::endl;
}

auto StreamEnvironment::setParallelism(size_t parallelism) -> void {
  if (parallelism > 0) {
    default_parallelism_ = parallelism;
  }
}

auto StreamEnvironment::addStream(std::shared_ptr<Stream> stream) -> void {
  streams_.push_back(std::move(stream));
}

}  // namespace candy
