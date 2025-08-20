#include "operator/join_operator_methods/eager/bruteforce.h"
#include "compute_engine/compute_engine.h"

namespace candy {

// 新的构造函数，支持KNN索引
BruteForceEager::BruteForceEager(int left_knn_index_id,
                                 int right_knn_index_id,
                                 double join_similarity_threshold,
                                 const std::shared_ptr<ConcurrencyManager> &concurrency_manager)
    : BaseMethod(join_similarity_threshold),
      left_knn_index_id_(left_knn_index_id),
      right_knn_index_id_(right_knn_index_id),
      concurrency_manager_(concurrency_manager),
      using_knn_(true) {
}

auto BruteForceEager::getOtherStreamKnnIndexId(int data_arrival_slot) const -> int {
    return (data_arrival_slot == 0) ? right_knn_index_id_ : left_knn_index_id_;
}

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

    // 修复：只有相似度大于等于阈值才进行join
    if (similarity >= join_similarity_threshold_) {
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

auto BruteForceEager::ExecuteEager(const VectorRecord& query_record, int slot)
  -> std::vector<std::unique_ptr<VectorRecord>> {
  if (!using_knn_ || !concurrency_manager_) {
    return std::vector<std::unique_ptr<VectorRecord>>();
  }

  int query_index_id = getOtherStreamKnnIndexId(slot);
  if (query_index_id == -1) {
    return std::vector<std::unique_ptr<VectorRecord>>();
  }

  // 使用KNN索引查询匹配的候选项
  std::vector<std::shared_ptr<const VectorRecord>> candidates =
      concurrency_manager_->query_for_join(query_index_id, query_record, join_similarity_threshold_);

  // 将候选项转换为独立的VectorRecord指针
  std::vector<std::unique_ptr<VectorRecord>> results;
  for (const auto& candidate : candidates) {
    if (candidate) {
      results.emplace_back(std::make_unique<VectorRecord>(*candidate));
    }
  }

  return results;
}

std::vector<std::unique_ptr<VectorRecord>> BruteForceEager::ExecuteLazy(
    const std::list<std::unique_ptr<VectorRecord>>& query_records,
    int query_slot) {

  if (!using_knn_ || !concurrency_manager_) {
    return std::vector<std::unique_ptr<VectorRecord>>();
  }

  int query_index_id = (query_slot == 0) ? right_knn_index_id_ : left_knn_index_id_;
  if (query_index_id == -1) {
    return std::vector<std::unique_ptr<VectorRecord>>();
  }

  std::vector<std::unique_ptr<VectorRecord>> all_results;

  // 对每个查询记录进行KNN索引查询
  for (const auto& query_record : query_records) {
    if (!query_record) continue;

    std::vector<std::shared_ptr<const VectorRecord>> candidates =
        concurrency_manager_->query_for_join(query_index_id, *query_record, join_similarity_threshold_);

    // 将候选项添加到结果中
    for (const auto& candidate : candidates) {
      if (candidate) {
        all_results.emplace_back(std::make_unique<VectorRecord>(*candidate));
      }
    }
  }

  return all_results;
}

}  // namespace candy