#pragma once
#include <memory>
#include <string>

#include "core/common/data_types.h"

namespace candy {
enum class FunctionType {
  None,
  Filter,
  Map,
  Join,
  Sink,
};

class Function {
 public:
  explicit Function(std::string name, FunctionType type) : name_(std::move(name)), type_(type) {}

  ~Function() = default;

  auto getName() const -> std::string { return name_; }

  auto getType() const -> FunctionType { return type_; }

  void setName(const std::string &name) { name_ = name; }

  void setType(const FunctionType type) { type_ = type; }

  virtual auto Execute(std::unique_ptr<VectorRecord> &record) -> std::unique_ptr<VectorRecord> { return nullptr; }

  virtual auto Execute(std::unique_ptr<VectorRecord> &left, std::unique_ptr<VectorRecord> &right)
      -> std::unique_ptr<VectorRecord> {
    return nullptr;
  }

 private:
  std::string name_;
  FunctionType type_ = FunctionType::None;
};
};  // namespace candy