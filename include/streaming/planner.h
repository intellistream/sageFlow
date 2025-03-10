#pragma once
#include <runtime/operator/log_operator.h>

#include "runtime/function/join_function.h"
#include "runtime/function/function.h"
#include "runtime/function/map_function.h"
#include "runtime/function/sink_function.h"

#include "streaming/logical_plan.h"
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
    for (const auto& functions = plan->GetFunctions(); auto& function : functions) {
      std::unique_ptr<Operator> next_op = nullptr;
      if (function->getType() == FunctionType::Join) {
        const auto join_function = dynamic_cast<JoinFunction*>(function.get());
        auto other_plan = std::move(join_function->GetOtherPlan());
        auto other_task = (*this)(other_plan);
        // TODO(pygone): Implement join operator
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