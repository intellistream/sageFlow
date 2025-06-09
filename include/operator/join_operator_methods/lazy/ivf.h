//
// Created by ZeroJustMe on 25-5-19.
//
#pragma once
#include "operator/join_operator_methods/base_method.h"
#include "index/ivf.h"
#include "concurrency/concurrency_manager.h"

namespace candy {

class IvfLazy final : public BaseMethod {
public:
  // Constructor now takes index IDs and ConcurrencyManager
  IvfLazy(int left_ivf_index_id,
          int right_ivf_index_id,
          double join_similarity_threshold,
          const std::shared_ptr<ConcurrencyManager> &concurrency_manager);

  ~IvfLazy() override = default;

  void Excute(std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
              std::unique_ptr<JoinFunction> &joinfuc,
              std::list<std::unique_ptr<VectorRecord>> &left_records,
              std::list<std::unique_ptr<VectorRecord>> &right_records) override;

  void Excute(std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
              std::unique_ptr<JoinFunction> &joinfuc,
              std::unique_ptr<VectorRecord> &data,
              std::list<std::unique_ptr<VectorRecord>> &records,
              int slot) override;

private:
  int left_ivf_index_id_;
  int right_ivf_index_id_;
  double join_similarity_threshold_;
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
  // Removed direct storage_manager_ and Ivf shared_ptrs as CM is now primary
};

} // namespace candy

