#pragma once
#include <vector>

#include "common/data_types.h"
#include "stream/data_stream_source/data_stream_source.h"

namespace candy {
class SiftStreamSource final : public DataStreamSource {
 public:
  explicit SiftStreamSource(std::string name);

  SiftStreamSource(std::string name, std::string file_path);

  void Init() override;

  auto Next() -> std::unique_ptr<VectorRecord> override;

 private:
  std::string file_path_;
  std::vector<std::unique_ptr<VectorRecord>> records_;
};
}  // namespace candy