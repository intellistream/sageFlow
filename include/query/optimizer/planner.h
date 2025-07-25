#pragma once

#include <memory>

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

  // 辅助方法：递归构建算子并添加到执行图
  std::shared_ptr<Operator> buildOperatorChain(const std::shared_ptr<Stream>& stream,
                                              ExecutionGraph* execution_graph,
                                              size_t default_parallelism) const;

  // 辅助方法：设置算子的并行度
  void configureOperatorParallelism(std::shared_ptr<Operator>& op,
                                    size_t default_parallelism) const;
};
}  // namespace candy