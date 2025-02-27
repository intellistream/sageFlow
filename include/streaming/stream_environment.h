#ifndef STREAM_ENVIRONMENT_HPP
#define STREAM_ENVIRONMENT_HPP

#include <core/utils/conf_map.h>
#include <runtime/operators/base_operators.h>
#include <streaming/data_stream.h>
#include <memory>
#include <string>
#include <vector>

namespace candy {

class StreamEnvironment {
public:
  // Constructor to initialize the environment
  explicit StreamEnvironment() = default;

  // Load configuration from a file
  auto loadConfiguration(const std::string &filePath) -> candy::ConfigMap;

  // Read a source and create a DataStream
  auto readSource(const std::string &sourceType, const std::string &path) -> std::shared_ptr<DataStream>;

  // Execute the pipeline
  void execute(const std::string &pipelineName);

  // Add an operator to the pipeline
  void addOperator(const std::shared_ptr<candy::BaseOperator> &op);

private:
  std::vector<std::shared_ptr<candy::BaseOperator>> operators_;
};

} // namespace candy

#endif // STREAM_ENVIRONMENT_HPP