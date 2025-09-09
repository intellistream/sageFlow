#ifndef STREAM_ENVIRONMENT_HPP
#define STREAM_ENVIRONMENT_HPP

#include <utils/conf_map.h>

#include <memory>
#include <string>
#include <vector>

#include "query/optimizer/planner.h"
#include "execution/execution_graph.h"

namespace candy {
class StreamEnvironment {
 public:
  // Constructor to initialize the environment
  explicit StreamEnvironment() {
    storage_manager_ = std::make_shared<StorageManager>();
    concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage_manager_);
    planner_ = std::make_shared<Planner>(concurrency_manager_);
    execution_graph_ = std::make_unique<ExecutionGraph>();
  }

  // Load configuration from a file
  static auto loadConfiguration(const std::string &file_path) -> ConfigMap;

  // 执行流处理任务（支持多线程）
  auto execute() -> void;

  // 停止执行
  auto stop() -> void;

  // 等待执行完成
  auto awaitTermination() -> void;

  auto addStream(std::shared_ptr<Stream> stream) -> void;

  // 设置全局并行度
  auto setParallelism(size_t parallelism) -> void;

  auto getStorageManager() -> std::shared_ptr<StorageManager> {
    return storage_manager_;
  }
  auto getConcurrencyManager() -> std::shared_ptr<ConcurrencyManager> {
    return concurrency_manager_;
  }
  auto getPlanner() -> std::shared_ptr<Planner> { return planner_; }

  // 重置环境：清空已注册的 streams/operators，重新创建 ExecutionGraph、存储与并发管理器
  // 用于测试循环中多次构建/执行，避免残留线程/队列/索引状态污染
  void reset();

 private:
  std::vector<std::shared_ptr<Stream>> streams_;
  std::vector<std::shared_ptr<Operator>> operators_;

  std::shared_ptr<StorageManager> storage_manager_;
  std::shared_ptr<Planner> planner_;
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
  std::unique_ptr<ExecutionGraph> execution_graph_;

  size_t default_parallelism_ = 1;
  bool is_running_ = false;
};

}  // namespace candy

#endif  // STREAM_ENVIRONMENT_HPP