
#pragma once

#include <core/common/data_types.h>  // Include VectorRecord definition
#include <runtime/function/function.h>

#include <memory>
#include <thread>
#include <vector>

namespace candy {
enum class OperatorType { NONE, OUTPUT, FILTER, MAP, JOIN, SINK }; // NOLINT

// Base class for all operators
class Operator {
 public:
  virtual ~Operator();

  explicit Operator(const OperatorType type);

  auto getType() const -> OperatorType;

  virtual auto open() -> void;

  virtual auto close() -> void;

  virtual auto process(std::unique_ptr<VectorRecord>& record, int slot = 0) -> bool;

  virtual void emit(const int id, std::unique_ptr<VectorRecord>& record) const;

  auto addChild(std::shared_ptr<Operator> child, const int slot = 0) -> int;

  std::vector<int> child2slot_;
  std::unique_ptr<Function> function_ = nullptr;
  std::vector<std::shared_ptr<Operator>> children_;
  OperatorType type_ = OperatorType::NONE;
  bool is_open_ = false;
};

}  // namespace candy
