#include <stream/stream_environment.h>

#include <memory>
#include <stdexcept>

namespace candy {

ConfigMap StreamEnvironment::global_conf;

void StreamEnvironment::setGlobalConfiguration(const ConfigMap &conf) { global_conf = conf; }

auto StreamEnvironment::getGlobalConfiguration() -> const ConfigMap & { return global_conf; }

void StreamEnvironment::setConfiguration(const ConfigMap &conf) { conf_ = conf; }

auto StreamEnvironment::getConfiguration() -> const ConfigMap & { return conf_; }

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
