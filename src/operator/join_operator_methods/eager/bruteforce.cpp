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
  // Create a new ComputeEngine for similarity calculation
  ComputeEngine engine;

  for (auto &rec : records) {
    if (!rec || !data) {
      continue;
    }

    // Calculate similarity using ComputeEngine
    double similarity = engine.Similarity(data->data_, rec->data_);

    // Only proceed with join if similarity is above threshold
    if (similarity < join_similarity_threshold_) {
      // Create copies for the join function
      auto rec_copy = std::make_unique<VectorRecord>(*rec);
      auto data_copy = std::make_unique<VectorRecord>(*data);

      auto response_rec = Response{ResponseType::Record, std::move(rec_copy)};
      auto response_data = Response{ResponseType::Record, std::move(data_copy)};

      auto ret = joinfuc->Execute(response_rec, response_data);
      if (ret.record_ != nullptr) {
        emit_pool.emplace_back(0, std::move(ret.record_));
      }
    }
  }
}

}  // namespace candy