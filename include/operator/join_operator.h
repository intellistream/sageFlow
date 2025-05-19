#pragma once

#include <functional>
#include <list>
#include <memory>

#include "common/data_types.h"
#include "operator/operator.h"
#include "operator/join_operator_methods/base_method.h"
#include "concurrency/concurrency_manager.h"

namespace candy {
  class JoinOperator final : public Operator {
   public:
    // Updated constructor to accept method name and concurrency manager
    explicit JoinOperator(std::unique_ptr<Function> &join_func,
                          const std::shared_ptr<ConcurrencyManager> &concurrency_manager,
                          const std::string& join_method_name = "bruteforce_lazy",
                          double join_similarity_threshold = 0.8);

    auto open() -> void override;

    auto process(Response &data, int slot) -> bool override;

    auto setMother(std::shared_ptr<Operator> mother) -> void;

    auto lazy_process(int slot) -> bool;

    auto eager_process(int slot) -> bool;

   private:
    auto clear_methods_return_pool() -> void;

    void initializeIVFIndexes(int nlist, double rebuild_threshold, int nprobes);

    std::unique_ptr<JoinFunction> join_func_;
    std::shared_ptr<Operator> mother_;
    std::unique_ptr<BaseMethod> join_method_;
    std::list<std::unique_ptr<VectorRecord>> left_records_;
    std::list<std::unique_ptr<VectorRecord>> right_records_;
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> methods_return_pool_;
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;

    // IVF specific members
    int left_ivf_index_id_;
    int right_ivf_index_id_;
    // std::shared_ptr<Ivf> left_ivf_index_;
    // std::shared_ptr<Ivf> right_ivf_index_;
    bool using_ivf_ = false;
    bool is_eager_ = false; // To help with index add/remove logic
    double join_similarity_threshold_ = 0.8; // Similarity threshold for join operations
  };
  }  // namespace candy
