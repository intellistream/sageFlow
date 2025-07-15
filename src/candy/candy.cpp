#include "candy/candy.h"

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "stream/data_stream_source/file_stream_source.h"
#include "stream/stream.h"

namespace candy {

Candy::Candy() = default;

void Candy::setConfig(const std::map<std::string, std::string> &config) {
  conf_ = ConfigMap(config);
  env_.setConfiguration(conf_);
}

auto Candy::createFileStreamSource(const std::string &name, const std::string &path) -> std::shared_ptr<Stream> {
  return std::make_shared<FileStreamSource>(name, path);
}

void Candy::execute() { env_.execute(); }

void Candy::addStream(std::shared_ptr<Stream> stream) { env_.addStream(std::move(stream)); }

}  // namespace candy
