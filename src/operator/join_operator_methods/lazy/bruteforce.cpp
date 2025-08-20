#include "operator/join_operator_methods/lazy/bruteforce.h"
#include "compute_engine/compute_engine.h"

namespace candy {

// 新的构造函数，支持KNN索引
BruteForceLazy::BruteForceLazy(int left_knn_index_id,
                               int right_knn_index_id,
                               double join_similarity_threshold,
                               const std::shared_ptr<ConcurrencyManager> &concurrency_manager)
    : BaseMethod(join_similarity_threshold),
      left_knn_index_id_(left_knn_index_id),
      right_knn_index_id_(right_knn_index_id),
      concurrency_manager_(concurrency_manager),
      using_knn_(true) {
}

void BruteForceLazy::Excute(
  std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
  std::unique_ptr<candy::JoinFunction> &joinfuc,
  std::list<std::unique_ptr<VectorRecord>> &left_records,
  std::list<std::unique_ptr<VectorRecord>> &right_records) {

  if (using_knn_ && concurrency_manager_) {
    // 使用KNN索引进行查询 - 使用左流查询右流索引
    int query_index_id = right_knn_index_id_;

    for (auto& left_record : left_records) {
      if (!left_record) continue;

      // 使用KNN索引查询相似向量
      std::vector<std::shared_ptr<const VectorRecord>> candidates =
          concurrency_manager_->query_for_join(query_index_id, *left_record, join_similarity_threshold_);

      // 对每个候选项执行join操作
      for (const auto &candidate : candidates) {
        if (candidate) {
          // 验证候选项是否在当前右侧窗口中
          bool found_in_window = false;
          for (const auto& right_record : right_records) {
            if (right_record && right_record->uid_ == candidate->uid_) {
              found_in_window = true;
              break;
            }
          }

          if (found_in_window) {
            auto left_copy = std::make_unique<VectorRecord>(*left_record);
            auto candidate_copy = std::make_unique<VectorRecord>(*candidate);

            Response response_left{ResponseType::Record, std::move(left_copy)};
            Response response_right{ResponseType::Record, std::move(candidate_copy)};

            Response result = joinfuc->Execute(response_left, response_right);
            if (result.record_) {
              emit_pool.emplace_back(0, std::move(result.record_));
            }
          }
        }
      }
    }
  } else {
    // Fallback到原有的暴力搜索逻辑
    ComputeEngine engine;
    for (auto &left : left_records) {
      for (auto &right : right_records) {
        if (!left || !right) {
          continue;
        }

        double similarity = engine.Similarity(left->data_, right->data_);
        if (similarity >= join_similarity_threshold_) {
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
}

void BruteForceLazy::Excute(
  std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
  std::unique_ptr<candy::JoinFunction> &joinfuc,
  std::unique_ptr<VectorRecord> &data,
  std::list<std::unique_ptr<VectorRecord>> &records,
  int slot) {
  // Lazy模式通常不使用单条记录处理，提供空实现
}

std::vector<std::unique_ptr<VectorRecord>> BruteForceLazy::ExecuteEager(
    const VectorRecord& query_record,
    int slot) {

  if (using_knn_ && concurrency_manager_) {
    int query_index_id = (slot == 0) ? right_knn_index_id_ : left_knn_index_id_;
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

  // 没有KNN索引时返回空结果
  return std::vector<std::unique_ptr<VectorRecord>>();
}

std::vector<std::unique_ptr<VectorRecord>> BruteForceLazy::ExecuteLazy(
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