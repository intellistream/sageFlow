#ifndef STREAM_ENVIRONMENT_HPP
#define STREAM_ENVIRONMENT_HPP

#include <utils/conf_map.h>

#include <memory>
#include <vector>

#include "compute_engine/compute_engine.h"
#include "concurrency/concurrency_manager.h"
#include "query/optimizer/planner.h"
#include "storage/storage_manager.h"

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

  auto getStorageManager() -> std::shared_ptr<StorageManager> {
    return storage_manager_;
  }
  auto getConcurrencyManager() -> std::shared_ptr<ConcurrencyManager> {
    return concurrency_manager_;
  }
  auto getPlanner() -> std::shared_ptr<Planner> { return planner_; }

 private:
  static ConfigMap global_conf_;
  ConfigMap conf_;
  std::vector<std::shared_ptr<Stream>> streams_;
  std::vector<std::shared_ptr<Operator>> operators_;

  std::shared_ptr<StorageManager> storage_manager_;
  std::shared_ptr<Planner> planner_;
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
};

}  // namespace candy

#endif  // STREAM_ENVIRONMENT_HPP