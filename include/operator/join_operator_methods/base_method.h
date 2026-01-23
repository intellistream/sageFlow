#pragma once
#include <list>
#include <deque>
#include <vector>
#include <unordered_set>

#include "function/join_function.h"

namespace sageFlow {

/// @brief Join 方法类型枚举（已简化，所有方法均为 Eager 模式）
enum class JoinMethodType {
  BRUTEFORCE,
  IVF,
  HNSW,
  HDR_TREE,
  LSH,
  CLUSTERED_JOIN,
  S3J
};

class BaseMethod {
 public:
  explicit BaseMethod(double join_similarity_threshold)
    : join_similarity_threshold_(join_similarity_threshold) {}

   virtual ~BaseMethod() = default;

   // 清理资源（如后台线程）
   virtual void close() {}

   // 原有接口保持兼容性
   virtual void Excute(std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
                       std::unique_ptr<JoinFunction> &joinfuc,
                       std::list<std::unique_ptr<VectorRecord>> &left_records,
                       std::list<std::unique_ptr<VectorRecord>> &right_records);

   /**
    * @brief 执行 Eager 模式的 Join 查询
    * 
    * 对单个查询记录，在对侧窗口中搜索满足相似度阈值的候选向量。
    * 所有 Join 方法均使用 Eager 模式，即每条记录到达时立即执行查询。
    * 
    * @param query_record 查询记录
    * @param query_slot 查询来源的 slot（0=左流，1=右流）
    * @param subtask_index 当前执行的 subtask 索引（用于 PartitionedWindowState 分区访问）
    * @return 满足阈值的候选向量列表
    */
   virtual std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
       const VectorRecord& query_record,
       int query_slot,
       size_t subtask_index = 0) = 0;

   virtual void Excute(std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
                       std::unique_ptr<JoinFunction> &joinfuc,
                       std::unique_ptr<VectorRecord> &data,
                       std::list<std::unique_ptr<VectorRecord>> &records,
                       int slot);

 protected:
  double join_similarity_threshold_;
  // 相似度计算参数 alpha（与 pipeline/JoinOperator 绑定，由工厂在构建策略时设置）
  // 仅用于：
  // - 调用 ConcurrencyManager::query_for_join(..., alpha) 时显式传参
  // - WindowState 暴力扫描等非索引路径的相似度计算
  double similarity_alpha_ = 0.1;

 public:
  void setSimilarityAlpha(double alpha) { similarity_alpha_ = alpha; }
  [[nodiscard]] double getSimilarityAlpha() const { return similarity_alpha_; }
 private:
};
}  // namespace sageFlow