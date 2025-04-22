#pragma once
#include <memory>
#include <string>

#include "common/data_types.h"
#include "stream/stream.h"

namespace candy {
enum class DataStreamSourceType {  // NOLINT
  None,
  File,

};

class DataStreamSource : public Stream {
 public:
  DataStreamSource(std::string name, DataStreamSourceType type);

  auto getType() const -> DataStreamSourceType;

  void setType(DataStreamSourceType type);

  void setBufferSizeLimit(size_t limit) { buffer_size_limit_ = limit; }
  size_t getBufferSizeLimit() const { return buffer_size_limit_; }

  virtual auto Next() -> std::unique_ptr<VectorRecord>  = 0;

  virtual void Init() {}

  DataStreamSourceType type_ = DataStreamSourceType::None;
  size_t buffer_size_limit_ = (1<<20);  // 1MB
};
}  // namespace candy