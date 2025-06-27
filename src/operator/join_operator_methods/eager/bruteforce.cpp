#include "operator/join_operator_methods/eager/bruteforce.h"
#include "compute_engine/compute_engine.h"

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
  auto temp_compute_engine = std::make_unique<ComputeEngine>();
  for (auto &rec : records) {
      auto rec_ptr = std::make_unique<VectorRecord>(*rec);
      auto data_ptr = std::make_unique<VectorRecord>(*data);
      auto similarity = temp_compute_engine->Similarity(rec_ptr->data_, data_ptr->data_);
      if (similarity < join_similarity_threshold_) {
        continue;
      }
      auto response_rec = Response{ResponseType::Record, std::move(rec_ptr)};
      auto response_data = Response{ResponseType::Record, std::move(data_ptr)};
      auto ret = joinfuc -> Execute(response_rec, response_data);
      auto &ret_record = ret.record_;
      if (ret_record != nullptr) {
        emit_pool.emplace_back(0, std::move(ret_record));
      }
  }
}

}