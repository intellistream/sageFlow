#ifndef STREAM_ENVIRONMENT_HPP
#define STREAM_ENVIRONMENT_HPP

#include <core/utils/conf_map.h>

#include <memory>
#include <string>
#include <vector>

#include "planner.h"

namespace candy {
extern std::unique_ptr<Planner> planner;

class StreamEnvironment {
 public:
  // Constructor to initialize the environment
  explicit StreamEnvironment() = default;

  // Load configuration from a file
  static auto loadConfiguration(const std::string &file_path) -> ConfigMap;

  auto execute() -> void;

  auto addStream(std::shared_ptr<Stream> stream) -> void;

 private:
  std::vector<std::shared_ptr<Stream>> streams_;
  std::vector<std::shared_ptr<Operator>> operators_;
};

}  // namespace candy

#endif  // STREAM_ENVIRONMENT_HPP