#pragma once

#include <memory>
#include <queue>
#include <thread>
#include <vector>
#include <string>

#include "common/data_types.h"  // Include VectorRecord definition
#include "function/function_api.h"

namespace candy {
enum class OperatorType {
  NONE,
  OUTPUT,
  FILTER,
  MAP,
  JOIN,
  SINK,
  TOPK,
  WINDOW,
  ITOPK,
  AGGREGATE,
};  // NOLINT

// Base class for all operators
class Operator {
 public:
  virtual ~Operator();

  explicit Operator(OperatorType type, size_t parallelism = 1);

  auto getType() const -> OperatorType;

  virtual auto open() -> void;

  virtual auto close() -> void;

  virtual auto process(Response& record, int slot) -> bool;

  virtual void emit(int id, Response& record) const;

  auto addChild(std::shared_ptr<Operator> child, int slot = 0) -> int;

  void set_parallelism(size_t p);

  auto get_parallelism() const -> size_t;

  std::vector<int> child2slot_;
  std::unique_ptr<Function> function_ = nullptr;
  std::vector<std::shared_ptr<Operator>> children_;
  OperatorType type_ = OperatorType::NONE;
  bool is_open_ = false;
  size_t parallelism_ = 1; // 默认并行度为 1
  bool is_available_ = true;  // Indicates if the operator is available for processing
  std::string name = "Operator"; // 添加name字段用于标识算子
};

}  // namespace candy
