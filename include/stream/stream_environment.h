#ifndef STREAM_ENVIRONMENT_HPP
#define STREAM_ENVIRONMENT_HPP

#include <utils/conf_map.h>

#include <memory>
#include <string>
#include <vector>

#include "query/optimizer/planner.h"

namespace candy {
class StreamEnvironment {
 public:
  // Constructor to initialize the environment
  explicit StreamEnvironment() {
    storage_manager_ = std::make_shared<StorageManager>();
    concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage_manager_);
    planner_ = std::make_shared<Planner>(concurrency_manager_);
  }

  // Load configuration from a file
  static auto loadConfiguration(const std::string &file_path) -> ConfigMap;

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
  std::vector<std::shared_ptr<Stream>> streams_;
  std::vector<std::shared_ptr<Operator>> operators_;

  std::shared_ptr<StorageManager> storage_manager_;
  std::shared_ptr<Planner> planner_;
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
};

}  // namespace candy

#endif  // STREAM_ENVIRONMENT_HPP