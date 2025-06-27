#include "operator/join_operator_methods/lazy/bruteforce.h"
#include "compute_engine/compute_engine.h"

namespace candy {
void BruteForceLazy::Excute(
  std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
  std::unique_ptr<candy::JoinFunction> &joinfuc,
  std::list<std::unique_ptr<VectorRecord>> &left_records,
  std::list<std::unique_ptr<VectorRecord>> &right_records) {
  // Create a new ComputeEngine for similarity calculation
  ComputeEngine engine;

  for (auto &left : left_records) {
    for (auto &right : right_records) {
      if (!left || !right) {
        continue;
      }

      // Calculate similarity using ComputeEngine
      double similarity = engine.Similarity(left->data_, right->data_);

      // Only proceed with join if similarity is above threshold
      if (similarity >= join_similarity_threshold_) {
        // Create copies for the join function (avoid moving originals)
        auto left_copy = std::make_unique<VectorRecord>(*left);
        auto right_copy = std::make_unique<VectorRecord>(*right);

        auto response_left = Response{ResponseType::Record, std::move(left_copy)};
        auto response_right = Response{ResponseType::Record, std::move(right_copy)};

        auto result = joinfuc->Execute(response_left, response_right);
        if (result.record_ != nullptr) {
          emit_pool.emplace_back(0, std::move(result.record_));
        }
      }
    }
  }
}
void BruteForceLazy::Excute(
  std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
  std::unique_ptr<candy::JoinFunction> &joinfuc,
  std::unique_ptr<VectorRecord> &data,
  std::list<std::unique_ptr<VectorRecord>> &records,
  int slot) {

}
}  // namespace candy
