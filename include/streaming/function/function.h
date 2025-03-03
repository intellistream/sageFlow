#pragma once
#include <memory>
#include <string>
#include <utility>

#include "core/common/data_types.h"

namespace candy {
enum class FunctionType { FILTER, MAP, JOIN, SINK };

class Function {
 public:
  Function(FunctionType type, std::string name) : type_(type), name_(std::move(name)) {}

  virtual ~Function() = default;

  auto getType() const -> FunctionType { return type_; }

  auto getName() const -> const std::string& { return name_; }

  virtual auto operator()(std::unique_ptr<VectorRecord>& data) -> bool = 0;

 private:
  FunctionType type_;
  std::string name_;
};
};  // namespace candy