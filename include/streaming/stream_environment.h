#ifndef STREAM_ENVIRONMENT_HPP
#define STREAM_ENVIRONMENT_HPP

#include <core/utils/conf_map.h>

#include <memory>
#include <string>
#include <vector>

#include "logical_plan.h"
#include "planner.h"

namespace candy {
extern std::unique_ptr<Planner> planner;

class StreamEnvironment {
 public:
  // Constructor to initialize the environment
  explicit StreamEnvironment() = default;

  // Load configuration from a file
  static auto loadConfiguration(const std::string &file_path) -> candy::ConfigMap;

  auto Register(const std::unique_ptr<LogicalPlan> &source) -> void;

  auto execute() -> void;

 private:
  std::vector<std::unique_ptr<Task>> tasks_;
};

}  // namespace candy

#endif  // STREAM_ENVIRONMENT_HPP