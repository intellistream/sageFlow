#include "operator/join_operator_methods/eager/bruteforce.h"

namespace candy {
void BruteForceEager::Excute(
  std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
  std::unique_ptr<candy::JoinFunction> &joinfuc,
  std::list<std::unique_ptr<VectorRecord>> &left_records,
  std::list<std::unique_ptr<VectorRecord>> &right_records) {

}

void BruteForceEager::Excute(
  std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
  std::unique_ptr<candy::JoinFunction> &joinfuc,
  std::unique_ptr<VectorRecord> &data,
  std::list<std::unique_ptr<VectorRecord>> &records,
  int slot) {
  for (auto &rec : records) {

      auto response_rec = Response{ResponseType::Record, std::make_unique<VectorRecord>(*rec)};
      auto response_data = Response{ResponseType::Record, std::make_unique<VectorRecord>(*data)};
      auto ret = joinfuc -> Execute(response_rec, response_data);
      auto &ret_record = ret.record_;
      if (ret_record != nullptr) {
        emit_pool.emplace_back(0, std::move(ret_record));
      }
  }
}

}