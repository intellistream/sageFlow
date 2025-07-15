#pragma once

#include <map>
#include <memory>
#include <string>

#include "stream/stream.h"
#include "stream/stream_environment.h"
#include "utils/conf_map.h"

namespace candy {

class Candy {
 public:
  Candy();
  void setConfig(const std::map<std::string, std::string> &config);
  auto createFileStreamSource(const std::string &name, const std::string &path) -> std::shared_ptr<Stream>;
  void execute();
  void addStream(std::shared_ptr<Stream> stream);

 private:
  StreamEnvironment env_;
  ConfigMap conf_;
};

}  // namespace candy
