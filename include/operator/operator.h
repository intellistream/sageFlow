#pragma once

#include <memory>
#include <optional>
#include <queue>
#include <thread>
#include <vector>
#include <string>

#include "common/data_types.h"  // Include VectorRecord definition
#include "function/function_api.h"
#include "execution/collector.h"
#include "execution/runtime_context.h"

namespace sageFlow {
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

  // Legacy open method (for backward compatibility)
  virtual auto open() -> void;

  // New open method with RuntimeContext
  virtual auto open(const RuntimeContext& context) -> void;

  virtual auto close() -> void;

  virtual auto process(Response&record, int slot) -> std::optional<Response>;

  // Legacy apply method (for backward compatibility)
  virtual auto apply(Response&& record, int slot, Collector& collector) -> void;

  // New apply method with RuntimeContext
  virtual auto apply(Response&& record, int slot, Collector& collector, 
                    const RuntimeContext& context) -> void;

  void set_parallelism(size_t p);

  auto get_parallelism() const -> size_t;

  std::unique_ptr<Function> function_ = nullptr;
  OperatorType type_ = OperatorType::NONE;
  bool is_open_ = false;
  size_t parallelism_ = 1; // 默认并行度为 1
  bool is_available_ = true;  // Indicates if the operator is available for processing
  std::string name = "Operator"; // 添加name字段用于标识算子
};

}  // namespace sageFlow
