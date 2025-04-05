#pragma once
#include <memory>
#include <string>

#include "common/data_types.h"

namespace candy {
enum class FunctionType {  // NOLINT
  None,
  Filter,
  Map,
  Join,
  Sink,
};

class Function {
 public:
  explicit Function(std::string name, FunctionType type);

  virtual ~Function();

  auto getName() const -> std::string;

  auto getType() const -> FunctionType;

  void setName(const std::string &name);

  void setType(FunctionType type);

  virtual auto Execute(std::unique_ptr<VectorRecord> &record) -> std::unique_ptr<VectorRecord>;

  virtual auto Execute(std::unique_ptr<VectorRecord> &left,
                       std::unique_ptr<VectorRecord> &right) -> std::unique_ptr<VectorRecord>;

 private:
  std::string name_;
  FunctionType type_ = FunctionType::None;
};
};  // namespace candy