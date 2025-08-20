#pragma once

#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <atomic>

#include "common/data_types.h"
#include "operator/operator.h"
#include "operator/join_operator_methods/base_method.h"
#include "concurrency/concurrency_manager.h"

namespace candy {
  class JoinOperator final : public Operator {
   public:
    explicit JoinOperator(std::unique_ptr<Function> &join_func,
                          const std::shared_ptr<ConcurrencyManager> &concurrency_manager,
                          const std::string& join_method_name = "bruteforce_lazy",
                          double join_similarity_threshold = 0.8);

    auto open() -> void override;

    auto process(Response&data, int slot) -> std::optional<Response> override;

    auto apply(Response&& record, int slot, Collector& collector) -> void override;

    auto setMother(std::shared_ptr<Operator> mother) -> void;

    auto lazy_process(int slot) -> std::optional<Response>;

    auto eager_process(int slot) -> std::optional<Response>;

   private:
    enum class InternalIndexKind { NONE, IVF, BRUTEFORCE, VAMANA };  // 可扩展

    void initializeIVFIndexes(int nlist, double rebuild_threshold, int nprobes); // 保留现有接口（暂未用到额外参数）
    bool createIndexPair(IndexType type, const std::string& prefix);

    // 线程安全的窗口更新方法
    auto updateSideThreadSafe(
        std::list<std::unique_ptr<VectorRecord>>& records,
        std::shared_mutex& records_mutex,
        int index_id_for_cc,
        std::unique_ptr<VectorRecord>& data_ptr,
        int64_t now_time_stamp,
        int slot) -> bool;

    // 获取候选项的辅助方法
    std::vector<std::unique_ptr<VectorRecord>> getCandidates(
        const std::unique_ptr<VectorRecord>& data_ptr, int slot);

    // 验证候选项是否在指定窗口中的辅助方法
    bool validateCandidateInWindow(
        const std::unique_ptr<VectorRecord>& candidate,
        const std::list<std::unique_ptr<VectorRecord>>& window_records);

    // 执行join操作的辅助方法
    void executeJoinForCandidates(
        const std::vector<std::unique_ptr<VectorRecord>>& candidates,
        const std::unique_ptr<VectorRecord>& data_ptr,
        int slot,
        std::vector<std::pair<int, std::unique_ptr<VectorRecord>>>& local_return_pool);

    // Lazy模式的join执行辅助方法
    void executeLazyJoin(
        const std::vector<std::unique_ptr<VectorRecord>>& candidates,
        int slot,
        std::vector<std::pair<int, std::unique_ptr<VectorRecord>>>& local_return_pool);

    std::unique_ptr<JoinFunction> join_func_;
    std::shared_ptr<Operator> mother_;
    std::unique_ptr<BaseMethod> join_method_;

    // 窗口记录
    std::list<std::unique_ptr<VectorRecord>> left_records_;
    std::list<std::unique_ptr<VectorRecord>> right_records_;
    mutable std::shared_mutex left_records_mutex_;
    mutable std::shared_mutex right_records_mutex_;

    std::shared_ptr<ConcurrencyManager> concurrency_manager_;

    // 通用索引 id（不再混用 IVF 命名）
    int left_index_id_  = -1;
    int right_index_id_ = -1;
    InternalIndexKind index_kind_ = InternalIndexKind::NONE;
    bool use_index_ = false;          // 是否使用底层索引（IVF / BruteForce / 未来扩展）
    bool is_eager_ = false;           // eager / lazy 模式
    double join_similarity_threshold_ = 0.8;
  };
  }  // namespace candy
