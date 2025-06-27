#include "operator/join_operator_methods/lazy/bruteforce.h"
#include "compute_engine/compute_engine.h"

namespace candy {
void BruteForceLazy::Excute(
  std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
  std::unique_ptr<candy::JoinFunction> &joinfuc,
  std::list<std::unique_ptr<VectorRecord>> &left_records,
  std::list<std::unique_ptr<VectorRecord>> &right_records) {
  auto temp_compute_engine = std::make_unique<ComputeEngine>();
  for (auto &left : left_records)
      for (auto &right : right_records) {
          auto left_ptr = std::make_unique<VectorRecord>(*left);
          auto right_ptr = std::make_unique<VectorRecord>(*right);
          auto similarity = temp_compute_engine->Similarity(left_ptr->data_, right_ptr->data_);
          if (similarity < join_similarity_threshold_) {
            continue;
          }
          auto response_left = Response{ResponseType::Record, std::move(left_ptr)};
          auto response_right = Response{ResponseType::Record, std::move(right_ptr)};
          auto result = joinfuc -> Execute(response_left, response_right);
          auto ret_record = std::make_unique<VectorRecord>(*result.record_);
          if (ret_record != nullptr) {
              emit_pool.emplace_back(0, std::move(ret_record));
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
