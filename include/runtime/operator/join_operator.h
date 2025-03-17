#pragma once

#include <functional>
#include <list>
#include <deque>
#include <memory>

#include "core/common/data_types.h"
#include "runtime/operator/operator.h"
#include "runtime/function/join_function.h"
#include "vector_db/vector_database.h"


namespace candy {
class JoinOperator final : public Operator {
 public:
  explicit JoinOperator(std::unique_ptr<Function> &join_func);

  auto open() -> void override;

  auto process(std::unique_ptr<VectorRecord> &data, const int slot) -> bool override;

  auto setMother(std::shared_ptr<Operator> mother) -> void;

  auto brute_process(std::unique_ptr<VectorRecord> &data, const int slot) -> bool;

 private:
  // anns index
  std::unique_ptr<JoinFunction> join_func_;
  std::shared_ptr<Operator> mother_;
  std::list<std::unique_ptr<VectorRecord>> left_records_;
  std::list<std::unique_ptr<VectorRecord>> right_records_;
  //VectorDatabase vector_db_;

};
}  // namespace candy