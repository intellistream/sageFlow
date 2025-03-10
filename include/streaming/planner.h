#pragma once
#include <runtime/operator/log_operator.h>

#include "runtime/function/function.h"
#include "runtime/function/join_function.h"
#include "runtime/operator/filter_operator.h"
#include "runtime/operator/join_operator.h"
#include "runtime/operator/map_operator.h"
#include "runtime/operator/sink_operator.h"
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
    for (auto& functions = plan->GetFunctions(); auto& function : functions) {
      std::unique_ptr<Operator> next_op = nullptr;
      if (function->getType() == FunctionType::Join) {
        const auto join_function = dynamic_cast<JoinFunction*>(function.get());
        auto other_plan = std::move(join_function->GetOtherPlan());
        auto other_task = (*this)(other_plan);
        // TODO(pygone): Implement join operator
        next_op = std::make_unique<LogOperator>();
      } else {
        if (function->getType() == FunctionType::Filter) {
          next_op = std::make_unique<FilterOperator>("filter", function);
        } else if (function->getType() == FunctionType::Map) {
          next_op = std::make_unique<MapOperator>("map", function);
        } else if (function->getType() == FunctionType::Sink) {
          next_op = std::make_unique<SinkOperator>("sink", function);
        }
      }
      if (!head) {
        head = std::move(next_op);
        op = head.get();
      } else {
        int id;
        op = op->set_next_operator(next_op, id);
      }
    }
    task->setOperator(head);
    return task;
  }
};
}  // namespace candy