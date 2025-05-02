
#pragma once

#include <memory>
#include <thread>
#include <vector>

#include "common/data_types.h"  // Include VectorRecord definition
#include "function/function_api.h"

namespace candy {
enum class OperatorType { NONE, OUTPUT, FILTER, MAP, JOIN, SINK,TOPK,WINDOW,ITOPK };  // NOLINT

// Base class for all operators
class Operator {
 public:
  virtual ~Operator();

  explicit Operator(OperatorType type);

  auto getType() const -> OperatorType;

  virtual auto open() -> void;

  virtual auto close() -> void;

  virtual auto process(Response& record, int slot = 0) -> bool;

  virtual void emit(int id, Response& record) const;

  auto addChild(std::shared_ptr<Operator> child, int slot = 0) -> int;

  std::vector<int> child2slot_;
  std::unique_ptr<Function> function_ = nullptr;
  std::vector<std::shared_ptr<Operator>> children_;
  OperatorType type_ = OperatorType::NONE;
  bool is_open_ = false;
};

}  // namespace candy
