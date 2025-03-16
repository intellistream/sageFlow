#pragma once
#include <memory>
#include <string>
#include <utility>

#include "core/common/data_types.h"

namespace candy {
enum class DataFlowType {
  None,
  File,
  Tcp,
};

class DataStream {
 public:
  DataStream(std::string name, DataFlowType type) : name_(std::move(name)), type_(type) {}

  auto getName() const -> std::string { return name_; }

  auto getType() const -> DataFlowType { return type_; }

  void setName(const std::string &name) { name_ = name; }

  void setType(const DataFlowType type) { type_ = type; }

  virtual auto Next(std::unique_ptr<VectorRecord> &record) -> bool = 0;
  virtual auto Init() -> void = 0;

 private:
  std::string name_;
  DataFlowType type_ = DataFlowType::None;
};
}  // namespace candy