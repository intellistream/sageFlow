#pragma once
#include <string>

#include "common/data_types.h"

namespace candy {
enum class FunctionType {  // NOLINT
  None,
  Source,
  Filter,
  Map,
  Join,
  Sink,
  Topk,
  Window,
  ITopk,
  IFilter,
  Aggregate,
};

class Function {
 public:
  explicit Function(std::string name, FunctionType type);

  virtual ~Function();

  auto getName() const -> std::string;

  auto getType() const -> FunctionType;

  void setName(const std::string &name);

  void setType(FunctionType type);

  virtual auto Execute(Response &resp) -> Response;

  virtual auto Execute(Response &left, Response &right) -> Response;

 private:
  std::string name_;
  FunctionType type_ = FunctionType::None;
};
};  // namespace candy