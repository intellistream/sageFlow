#include <streaming/data_stream.h>
#include <streaming/stream_environment.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace candy {
auto StreamEnvironment::loadConfiguration(const std::string &filePath) -> ConfigMap {
  ConfigMap config;
  if (!config.fromFile(filePath)) {
    throw std::runtime_error("Failed to load configuration from: " + filePath);
  }
  return config;
}

auto StreamEnvironment::readSource(const std::string &sourceType,
                                   const std::string &path) -> std::shared_ptr<DataStream> {
  auto stream = std::make_shared<DataStream>(sourceType);
  stream->addRecord(std::make_shared<VectorRecord>("1", VectorData{1.0, 2.0, 3.0}, 0));
  return stream;
}

void StreamEnvironment::execute(const std::string &pipelineName) {
  if (operators_.empty()) {
    throw std::runtime_error("Pipeline execution failed: No operators defined.");
  }

  for (const auto &op : operators_) {
    op->open();
    op->process(nullptr);  // Replace with actual data in the pipeline flow
    op->close();
  }
}

void StreamEnvironment::addOperator(const std::shared_ptr<BaseOperator> &op) { operators_.emplace_back(op); }

}  // namespace candy
