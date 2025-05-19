#include "operator/join_operator_methods/lazy/bruteforce.h"

namespace candy {
void BruteForceLazy::Excute(
  std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
  std::unique_ptr<candy::JoinFunction> &joinfuc,
  std::list<std::unique_ptr<VectorRecord>> &left_records_,
  std::list<std::unique_ptr<VectorRecord>> &right_records_) {
  for (auto &left : left_records_)
      for (auto &right : right_records_) {
          auto response_left = Response{ResponseType::Record, std::move(left)};
          auto response_right = Response{ResponseType::Record, std::move(right)};
          auto result = joinfuc -> Execute(response_left, response_right);
          auto &result_record = result.record_;
          if (result_record != NULL)
              emit_pool.emplace_back(0, std::move(result_record));
      }

}
void BruteForceLazy::Excute(
  std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
  std::unique_ptr<candy::JoinFunction> &joinfuc,
  std::unique_ptr<VectorRecord> &data,
  std::list<std::unique_ptr<VectorRecord>> &records_,
  int slot) {

}
}