#pragma once
#include <runtime/operators/log_operator.h>

#include "function/join.h"
#include "streaming/task/task.h"

namespace candy {

class Planner {
 public:
  Planner() = default;

  auto operator()(const std::unique_ptr<LogicalPlan>& plan) const -> std::unique_ptr<Task> {
    auto task = std::make_unique<Task>();
    std::unique_ptr<Operator> head = nullptr;
    Operator* op = nullptr;
    task->setDataStream(plan->GetDataStream());
    for (const auto& transformations = plan->GetTransformations(); auto& transformation : transformations) {
      std::unique_ptr<Operator> next_op = nullptr;
      if (transformation->getType() == FunctionType::JOIN) {
        const auto join_function = dynamic_cast<JoinFunction*>(transformation.get());
        auto other_plan = std::move(join_function->GetOtherPlan());
        auto other_task = (*this)(other_plan);
        // TODO: Implement join operator
        next_op = std::make_unique<LogOperator>();
      } else {
        next_op = std::make_unique<LogOperator>();
      }
      if (!head) {
        head = std::move(next_op);
        op = head.get();
      } else {
        op = op->set_next_operator(next_op);
      }
    }
    task->setOperator(head);
    return task;
  }
};
}  // namespace candy