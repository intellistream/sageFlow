#include <stream/stream_environment.h>

#include <iostream>
#include "utils/logger.h"
#include "utils/log_config.h"
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
  CANDY_LOG_WARN("ENV", "StreamEnvironment already running");
    return;
  }

  CANDY_LOG_INFO("ENV", "Building execution graph streams={} ", streams_.size());

  // 先为每个根源流按顺序分配 slotId，并写入 Stream 上
  int next_slot = 0;
  for (auto &stream : streams_) {
    if (stream->function_ == nullptr) { // 源流
      stream->setSlotId(next_slot++);
    }
  }
  // 使用Planner构建执行图而不是单独的算子（Planner 将读取各 Stream 的 slotId）
  for (auto &stream : streams_) {
    planner_->planToExecutionGraph(stream, execution_graph_.get(), default_parallelism_);
  }

  // 构建执行图
  execution_graph_->buildGraph();

  // 启动多线程执行
  execution_graph_->start();
  is_running_ = true;

  CANDY_LOG_INFO("ENV", "StreamEnvironment started");
  
  // 若配置中存在扁平 key "log.level" 则应用；否则环境变量 CANDY_LOG_LEVEL 覆盖
  try {
    if (config_.exist("log.level")) {
      std::string lvl = std::get<std::string>(config_.getValue("log.level"));
      init_log_level(lvl);
    } else {
      init_log_level("");
    }
  } catch(const std::exception &e) {
    CANDY_LOG_WARN("ENV", "log_level_config_failed what={} ", e.what());
  }
}

auto StreamEnvironment::stop() -> void {
  if (!is_running_) {
    return;
  }

  CANDY_LOG_INFO("ENV", "Stopping StreamEnvironment...");
  execution_graph_->stop();
  is_running_ = false;
}

auto StreamEnvironment::awaitTermination() -> void {
  // 即使 is_running_ 已变为 false（stop 后），也需要 join，确保线程收敛
  execution_graph_->join();
  is_running_ = false;
  CANDY_LOG_INFO("ENV", "StreamEnvironment terminated");
}

auto StreamEnvironment::setParallelism(size_t parallelism) -> void {
  if (parallelism > 0) {
    default_parallelism_ = parallelism;
  }
}

auto StreamEnvironment::addStream(std::shared_ptr<Stream> stream) -> void {
  streams_.push_back(std::move(stream));
}

void StreamEnvironment::reset() {
  // 若在运行中，先尝试停止并等待
  if (is_running_) {
    try { stop(); } catch (...) {}
    try { awaitTermination(); } catch (...) {}
  }
  // 清空已注册流与算子
  streams_.clear();
  operators_.clear();
  // 重新构建核心组件（存储/并发管理/执行图/Planner）
  storage_manager_ = std::make_shared<StorageManager>();
  concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage_manager_);
  planner_ = std::make_shared<Planner>(concurrency_manager_);
  execution_graph_ = std::make_unique<ExecutionGraph>();
  is_running_ = false;
  CANDY_LOG_INFO("ENV", "StreamEnvironment reset completed");
}

}  // namespace candy
