#ifndef STREAM_ENVIRONMENT_HPP
#define STREAM_ENVIRONMENT_HPP

#include <utils/conf_map.h>

#include <memory>
#include <vector>
#include <string>

#include "compute_engine/compute_engine.h"
#include "concurrency/concurrency_manager.h"
#include "query/optimizer/planner.h"
#include "storage/storage_manager.h"
#include "function/source_function.h"

namespace candy {
class StreamEnvironment {
 public:
  // Constructor to initialize the environment
  explicit StreamEnvironment() {
    auto compute_engine = std::make_shared<ComputeEngine>();
    storage_manager_ = std::make_shared<StorageManager>(compute_engine);
    concurrency_manager_ =
        std::make_shared<ConcurrencyManager>(storage_manager_);
    concurrency_manager_->setEngine(compute_engine);
    planner_ = std::make_shared<Planner>(concurrency_manager_);
  }

  static void setGlobalConfiguration(const ConfigMap &conf);
  static auto getGlobalConfiguration() -> const ConfigMap &;

  void setConfiguration(const ConfigMap &conf);
  auto getConfiguration() -> const ConfigMap &;

  auto execute() -> void;

  auto addStream(std::shared_ptr<Stream> stream) -> void;

  // Factory method to create source streams
  auto createSourceStream(const std::string& name, std::unique_ptr<SourceFunction> source_func) -> std::shared_ptr<Stream>;

  // Convenience method for file streams
  auto createFileStream(const std::string& name, const std::string& file_path) -> std::shared_ptr<Stream>;

  auto getStorageManager() -> std::shared_ptr<StorageManager> {
    return storage_manager_;
  }
  auto getConcurrencyManager() -> std::shared_ptr<ConcurrencyManager> {
    return concurrency_manager_;
  }
  auto getPlanner() -> std::shared_ptr<Planner> { return planner_; }

 private:
  static ConfigMap global_conf;
  ConfigMap conf_;
  std::vector<std::shared_ptr<Stream>> streams_;
  std::vector<std::shared_ptr<Operator>> operators_;

  std::shared_ptr<StorageManager> storage_manager_;
  std::shared_ptr<Planner> planner_;
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
};

}  // namespace candy

#endif  // STREAM_ENVIRONMENT_HPP