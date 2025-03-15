#pragma once
#include <memory>
#include <string>

#include "core/common/data_types.h"
#include "streaming/stream.h"

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

  virtual auto Next() -> std::unique_ptr<VectorRecord>  = 0;

  virtual void Init() {}

  DataStreamSourceType type_ = DataStreamSourceType::None;
};
}  // namespace candy