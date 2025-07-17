#include <stream/stream_environment.h>

#include <memory>
#include <stdexcept>

#include "function/file_source_function.h"
#include "operator/source_operator.h"

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

auto StreamEnvironment::createSourceStream(const std::string& name, std::unique_ptr<SourceFunction> source_func) -> std::shared_ptr<Stream> {
  // Create source operator with the source function
  auto source_op = std::make_shared<SourceOperator>(std::move(source_func));
  
  // Create a stream that wraps this source operator
  auto stream = std::make_shared<Stream>(name);
  stream->source_operator_ = source_op;
  
  return stream;
}

auto StreamEnvironment::createFileStream(const std::string& name, const std::string& file_path) -> std::shared_ptr<Stream> {
  auto file_source = std::make_unique<FileSourceFunction>(name + "_source", file_path);
  return createSourceStream(name, std::move(file_source));
}

}  // namespace candy
