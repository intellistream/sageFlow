#pragma once

#include <functional>
#include <list>
#include <memory>

#include "common/data_types.h"
#include "operator/operator.h"

namespace candy {
class JoinOperator final : public Operator {
 public:
  explicit JoinOperator(std::unique_ptr<Function> &join_func);

  auto open() -> void override;

  auto process(Response &data, int slot) -> bool override;

  auto setMother(std::shared_ptr<Operator> mother) -> void;

 private:
  // anns index
  std::unique_ptr<Function> join_func_;
  std::shared_ptr<Operator> mother_;
  std::list<std::unique_ptr<VectorRecord>> left_records_;
  std::list<std::unique_ptr<VectorRecord>> right_records_;
};
}  // namespace candy