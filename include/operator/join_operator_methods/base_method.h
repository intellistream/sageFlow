#pragma once
#include <list>
#include <vector>
#include <unordered_set>

#include "function/join_function.h"

namespace candy {

enum class JoinMethodType {
  BRUTEFORCE_EAGER,
  BRUTEFORCE_LAZY,
  IVF_EAGER,
  IVF_LAZY
};

class BaseMethod {
 public:
  explicit BaseMethod(double join_similarity_threshold)
    : join_similarity_threshold_(join_similarity_threshold){}

   virtual ~BaseMethod() = default;

   // 原有接口保持兼容性
   virtual void Excute(std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
                       std::unique_ptr<JoinFunction> &joinfuc,
                       std::list<std::unique_ptr<VectorRecord>> &left_records,
                       std::list<std::unique_ptr<VectorRecord>> &right_records);

   // 新的优化接口：Eager模式 - 只传递单个查询记录，返回所有候选结果
   virtual std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
       const VectorRecord& query_record,
       int query_slot) = 0;

   // 新的优化接口：Lazy模式 - 传递一批查询记录，返回所有候选结果
   virtual std::vector<std::unique_ptr<VectorRecord>> ExecuteLazy(
       const std::list<std::unique_ptr<VectorRecord>>& query_records,
       int query_slot) = 0;

   virtual void Excute(std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
                       std::unique_ptr<JoinFunction> &joinfuc,
                       std::unique_ptr<VectorRecord> &data,
                       std::list<std::unique_ptr<VectorRecord>> &records,
                       int slot);

 protected:
  double join_similarity_threshold_;
 private:
};
}  // namespace candy