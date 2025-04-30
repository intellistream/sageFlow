#pragma once

#include <functional>
#include <list>
#include <memory>

#include "common/data_types.h"
#include "operator/operator.h"
#include "operator/join_operator_methods/base_method.h"
#include "operator/join_operator_methods/join_methods.h"

namespace candy {
  class JoinOperator final : public Operator {
   public:
    explicit JoinOperator(std::unique_ptr<Function> &join_func);
  
    auto open() -> void override;
  
    auto process(Response &data, const int slot) -> bool override;
  
    auto setMother(std::shared_ptr<Operator> mother) -> void;
  
    auto lazy_process(const int slot) -> bool;
  
    auto eager_process(const int slot) -> bool;
  
   private:
    
    auto clear_methods_return_pool() -> void;
  
    std::unique_ptr<JoinFunction> join_func_;
    std::shared_ptr<Operator> mother_;
    std::unique_ptr<BaseMethod> join_method_;
    std::list<std::unique_ptr<VectorRecord>> left_records_;
    std::list<std::unique_ptr<VectorRecord>> right_records_;
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> methods_return_pool;
    //VectorDatabase vector_db_;
  
  };
  }  // namespace candy