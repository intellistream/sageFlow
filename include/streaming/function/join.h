#pragma once
#include <streaming/logical_plan.h>

#include <functional>
#include <memory>

#include "core/common/data_types.h"
#include "streaming/function/function.h"

namespace candy {
class JoinFunction : public Function {
 public:
  JoinFunction(const std::string &name,
               std::function<bool(std::unique_ptr<VectorRecord> &, std::unique_ptr<VectorRecord> &)> &join_func,
               std::unique_ptr<LogicalPlan> &other_plan)
      : Function(FunctionType::JOIN, name), join_func_(std::move(join_func)), other_plan_(std::move(other_plan)) {}

  auto operator()(std::unique_ptr<VectorRecord> &data) -> bool override { return join_func_(data, data); }
  auto GetOtherPlan() -> std::unique_ptr<LogicalPlan> & { return other_plan_; }
 private:
  std::function<bool(std::unique_ptr<VectorRecord> &, std::unique_ptr<VectorRecord> &)> join_func_;
  std::unique_ptr<LogicalPlan> other_plan_;
};
}  // namespace candy