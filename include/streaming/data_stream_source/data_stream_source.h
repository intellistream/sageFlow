#pragma once
#include <memory>
#include <string>
#include <utility>

#include "core/common/data_types.h"
#include "streaming/stream.h"

namespace candy {
enum class DataStreamSourceType {  // NOLINT
  None,
  File,

};

class DataStreamSource : public Stream {
 public:
  DataStreamSource(std::string name, const DataStreamSourceType type) : Stream(std::move(name)), type_(type) {}

  auto getType() const -> DataStreamSourceType { return type_; }

  void setType(const DataStreamSourceType type) { type_ = type; }

  virtual auto Next() -> std::unique_ptr<VectorRecord> = 0;

  virtual void Init() {}

  DataStreamSourceType type_ = DataStreamSourceType::None;
};
}  // namespace candy