#pragma once
#include <utility>
#include <vector>

#include "core/common/data_types.h"
#include "streaming/data_stream_source/data_stream_source.h"

namespace candy {
class FileStreamSource : public DataStreamSource {
 public:
  explicit FileStreamSource(std::string name);

  FileStreamSource(std::string name, std::string file_path);

  void Init() override;

  auto Next() -> std::unique_ptr<VectorRecord> override;

 private:
  std::string file_path_;
  std::vector<std::unique_ptr<VectorRecord>> records_;
};
}  // namespace candy