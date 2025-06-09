#include "operator/join_operator_methods/lazy/bruteforce.h"

namespace candy {
void BruteForceLazy::Excute(
  std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
  std::unique_ptr<candy::JoinFunction> &joinfuc,
  std::list<std::unique_ptr<VectorRecord>> &left_records,
  std::list<std::unique_ptr<VectorRecord>> &right_records) {
  for (auto &left : left_records)
      for (auto &right : right_records) {
          auto response_left = Response{ResponseType::Record, std::move(left)};
          auto response_right = Response{ResponseType::Record, std::move(right)};
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