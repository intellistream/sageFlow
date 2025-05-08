#pragma once
#include <memory>
#include <string>
#include <vector>

#include "common/data_types.h"
#include "function/function_types.h"

namespace candy {

class Function {
 public:
  explicit Function(std::string name, FunctionType type);

  // Constructor with only type parameter
  explicit Function(FunctionType type);

  virtual ~Function();

  auto getName() const -> std::string;

  auto getType() const -> FunctionType;

  void setName(const std::string &name);

  void setType(FunctionType type);

  virtual auto Execute(DataElement &element) -> DataElement;

  virtual auto Execute(DataElement &left, DataElement &right) -> DataElement;

 private:
  std::string name_;
  FunctionType type_ = FunctionType::None;
};
}  // namespace candy