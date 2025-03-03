#include <streaming/logical_plan.h>
#include <streaming/stream_environment.h>

#include <memory>
#include <stdexcept>
#include <string>

namespace candy {
std::unique_ptr<Planner> planner = std::make_unique<Planner>();

auto StreamEnvironment::loadConfiguration(const std::string &file_path) -> ConfigMap {
  ConfigMap config;
  if (!config.fromFile(file_path)) {
    throw std::runtime_error("Failed to load configuration from: " + file_path);
  }
  return config;
}

auto StreamEnvironment::Register(const std::unique_ptr<LogicalPlan> &source) -> void {
  auto task = (*planner)(source);
  tasks_.emplace_back(std::move(task));
}

auto StreamEnvironment::execute() -> void {
  for (const auto &task : tasks_) {
    task->begin();
  }
}

}  // namespace candy
