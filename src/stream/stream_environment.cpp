#include <stream/stream_environment.h>

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

auto StreamEnvironment::execute() -> void {
  if (streams_.empty()) {
    throw std::runtime_error("No streams to execute.");
  }
  for (auto &stream : streams_) {
    auto op = planner_->plan(stream);
    operators_.push_back(std::move(op));
  }
  for (const auto &op : operators_) {
    op->open();
  }
}

auto StreamEnvironment::addStream(std::shared_ptr<Stream> stream) -> void { streams_.push_back(std::move(stream)); }

}  // namespace candy
