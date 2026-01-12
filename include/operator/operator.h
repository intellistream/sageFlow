#pragma once

#include <memory>
#include <optional>
#include <queue>
#include <thread>
#include <vector>
#include <string>
#include <atomic>
#include <mutex>

#include "common/data_types.h"  // Include VectorRecord definition
#include "function/function_api.h"
#include "execution/collector.h"
#include "execution/runtime_context.h"
#include "execution/partitioner.h"

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

  /**
   * @brief 获取算子期望的输入分区器
   * 
   * 下游算子可以重写此方法来指定期望的分区策略。
   * 默认返回 nullptr，表示使用默认的 RoundRobin 分区器。
   * 
   * @param dimension 向量维度（某些分区器需要）
   * @param num_partitions 分区数量
   * @return 分区器实例，或 nullptr 使用默认
   */
  virtual std::unique_ptr<IPartitioner> getPreferredPartitioner(
      int dimension = 0, int num_partitions = 0) const;

  std::unique_ptr<Function> function_ = nullptr;
  OperatorType type_ = OperatorType::NONE;
  bool is_open_ = false;
  size_t parallelism_ = 1; // 默认并行度为 1
  bool is_available_ = true;  // Indicates if the operator is available for processing
  std::string name = "Operator"; // 添加name字段用于标识算子

  // ------------------------------------------------------------------
  // Lifecycle coordination for multi-vertex execution:
  // ExecutionGraph creates multiple ExecutionVertex threads that may share
  // the same Operator instance (shared_ptr). These fields allow ExecutionVertex
  // to ensure open() runs once and close() runs once after the last vertex.
  // ------------------------------------------------------------------
  mutable std::once_flag open_once_;
  std::atomic<size_t> active_vertices_{0};
};

}  // namespace sageFlow
