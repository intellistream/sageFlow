#include <list>
#include <vector>
#include <memory>

#include "operator/join_operator_methods/base_method.h"
#include "function/join_function.h"
#include "concurrency/concurrency_manager.h"

namespace candy {
class BruteForceEager : public BaseMethod {
 public:
  // 更新构造函数，支持ConcurrencyManager
  explicit BruteForceEager(double join_similarity_threshold) : BaseMethod(join_similarity_threshold) {}

  // 新的构造函数，支持KNN索引
  BruteForceEager(int left_knn_index_id,
                  int right_knn_index_id,
                  double join_similarity_threshold,
                  const std::shared_ptr<ConcurrencyManager> &concurrency_manager);

  ~BruteForceEager() override = default;

  void Excute(std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
              std::unique_ptr<JoinFunction> &joinfuc,
              std::list<std::unique_ptr<VectorRecord>> &left_records,
              std::list<std::unique_ptr<VectorRecord>> &right_records) override;

  void Excute(std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
              std::unique_ptr<JoinFunction> &joinfuc,
              std::unique_ptr<VectorRecord> &data,
              std::list<std::unique_ptr<VectorRecord>> &records,
              int slot) override;

  // 新的优化接口
  std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
      const VectorRecord& query_record,
      int slot) override;

  std::vector<std::unique_ptr<VectorRecord>> ExecuteLazy(
      const std::list<std::unique_ptr<VectorRecord>>& query_records,
      int query_slot) override;

  // 辅助方法
  auto getOtherStreamKnnIndexId(int data_arrival_slot) const -> int;

 private:
  // KNN索引相关成员
  int left_knn_index_id_ = -1;
  int right_knn_index_id_ = -1;
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
  bool using_knn_ = false;
};
}  // namespace candy