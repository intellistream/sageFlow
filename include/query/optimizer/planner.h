#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "concurrency/concurrency_manager.h"
#include "operator/operator_api.h"
#include "execution/execution_graph.h"

namespace candy {

class Planner {
 public:
  explicit Planner(const std::shared_ptr<ConcurrencyManager>& concurrency_manager);

  // 将Stream计划为ExecutionGraph（多线程版本）
  void planToExecutionGraph(const std::shared_ptr<Stream>& stream,
                            ExecutionGraph* execution_graph,
                            size_t default_parallelism) const;


 private:
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;

  // 已构建的流 -> 其链路的头尾算子，避免重复构建
  struct BuiltChainInfo {
    std::shared_ptr<Operator> head;
    std::shared_ptr<Operator> tail;
  };
  mutable std::unordered_map<const Stream*, BuiltChainInfo> built_chains_;

    // 辅助方法：递归构建算子并添加到执行图，使用 Stream 已设置的 slotId
  // 返回该流链路的“尾算子”（默认一头一尾）
  std::shared_ptr<Operator> buildOperatorChain(const std::shared_ptr<Stream>& stream,
                         ExecutionGraph* execution_graph,
                         size_t default_parallelism,
                         int inherited_slot_id) const;

  // 辅助方法：设置算子的并行度
  void configureOperatorParallelism(std::shared_ptr<Operator>& op,
                                    size_t default_parallelism) const;

};
}  // namespace candy