#pragma once
#include "runtime/function/function.h"
#include "streaming/logical_plan.h"

namespace candy {
using JoinFunc = std::function<bool(std::unique_ptr<VectorRecord> &, std::unique_ptr<VectorRecord> &)>;

class JoinFunction : public Function {
 public:
  explicit JoinFunction(std::string name) : Function(std::move(name), FunctionType::Join) {}

  JoinFunction(std::string name, JoinFunc join_func)
      : Function(std::move(name), FunctionType::Join), join_func_(std::move(join_func)) {}

  auto Execute(std::unique_ptr<VectorRecord> &left,
               std::unique_ptr<VectorRecord> &right) -> std::unique_ptr<VectorRecord> override {
    if (join_func_(left, right)) {
      return std::move(left);
    }
    return nullptr;
  }

  auto setJoinFunc(JoinFunc join_func) -> void { join_func_ = std::move(join_func); }

  auto GetOtherPlan() -> std::unique_ptr<LogicalPlan> & { return other_plan_; }

  auto setOtherPlan(std::unique_ptr<LogicalPlan> other_plan) -> void { other_plan_ = std::move(other_plan); }


 private:
  JoinFunc join_func_;
  std::unique_ptr<LogicalPlan> other_plan_ = nullptr;
};
};  // namespace candy