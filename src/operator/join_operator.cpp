/*
修改 Eager/Lazy 通过process 中的 IsEagerAlgorithm 修改
修改调用的方法， 则修改 using JoinWay 后面的等于号
*/
#include "operator/join_operator.h"
#include "operator/join_operator_methods/join_methods.h"
#include "operator/join_metrics.h"

#include <cassert>
#include <iostream>
#include <algorithm>

#include "utils/logger.h"

#include "spdlog/fmt/bundled/chrono.h"

namespace candy {

// 旧接口保留（如果未来需要 IVF 特有参数，可扩展重写）
void JoinOperator::initializeIVFIndexes(int /*nlist*/, double /*rebuild_threshold*/, int /*nprobes*/) {
    // 已由通用 createIndexPair 取代，这里留空以兼容旧调用（防止外部引用报错）
}

bool JoinOperator::createIndexPair(IndexType type, const std::string& prefix) {
    if (!concurrency_manager_) return false;
    left_index_id_ = concurrency_manager_->create_index(prefix + "_left", type, join_func_->getDim());
    right_index_id_ = concurrency_manager_->create_index(prefix + "_right", type, join_func_->getDim());
    return left_index_id_ != -1 && right_index_id_ != -1;
}

static inline std::string to_lower_copy(std::string v) {
    std::transform(v.begin(), v.end(), v.begin(), [](unsigned char c){return char(std::tolower(c));});
    return v;
}

JoinOperator::JoinOperator(std::unique_ptr<Function> &join_func,
                           const std::shared_ptr<ConcurrencyManager> &concurrency_manager,
                           const std::string& join_method_name_raw,
                           double join_similarity_threshold)
    : Operator(OperatorType::JOIN), concurrency_manager_(concurrency_manager),
      join_similarity_threshold_(join_similarity_threshold) {
    join_func_ = std::unique_ptr<JoinFunction>(dynamic_cast<JoinFunction*>(join_func.release()));
    if (!join_func_) {
        throw std::runtime_error("JoinOperator: join_func is not a JoinFunction");
    }
    if (!concurrency_manager_) {
        throw std::runtime_error("JoinOperator: concurrency_manager is a nullptr");
    }

    std::string join_method_name = to_lower_copy(join_method_name_raw);

    // 解析模式（_eager / _lazy）
    if (join_method_name.rfind("_eager") != std::string::npos) {
        is_eager_ = true;
    } else if (join_method_name.rfind("_lazy") != std::string::npos) {
        is_eager_ = false;
    } else {
        // 未指定时默认 lazy
        is_eager_ = false;
    }

    // 提取算法前缀（截掉最后一个 '_' 之后的部分）
    std::string algo = join_method_name;
    auto pos = algo.rfind('_');
    if (pos != std::string::npos) algo = algo.substr(0, pos);

    if (algo == "ivf") {
        index_kind_ = InternalIndexKind::IVF;
        if (createIndexPair(IndexType::IVF, "join_ivf")) {
            use_index_ = true;
            join_method_ = std::make_unique<IvfJoinMethod>(left_index_id_, right_index_id_,
                                                 join_similarity_threshold_, concurrency_manager_);
        } else {
            index_kind_ = InternalIndexKind::NONE;
            use_index_ = false;
            join_method_ = std::make_unique<BruteForceJoinMethod>(-1, -1, join_similarity_threshold_, concurrency_manager_);
        }
    } else if (algo == "bruteforce" || algo == "bf" ) {
        index_kind_ = InternalIndexKind::BRUTEFORCE;
        if (createIndexPair(IndexType::BruteForce, "join_bf")) {
            use_index_ = true;
            join_method_ = std::make_unique<BruteForceJoinMethod>(left_index_id_, right_index_id_,
                                                        join_similarity_threshold_, concurrency_manager_);
        } else {
            index_kind_ = InternalIndexKind::NONE;
            use_index_ = false;
            join_method_ = std::make_unique<BruteForceJoinMethod>(-1, -1, join_similarity_threshold_, concurrency_manager_);
        }
    } else {
        index_kind_ = InternalIndexKind::NONE;
        use_index_ = false;
        is_eager_ = false;
        join_method_ = std::make_unique<BruteForceJoinMethod>(-1, -1, join_similarity_threshold_, concurrency_manager_);
    }
}

void JoinOperator::open() {
  if (is_open_) return;
  is_open_ = true;
}

auto JoinOperator::updateSideThreadSafe(
    std::deque<std::unique_ptr<VectorRecord>>& records,
    std::shared_mutex& records_mutex,
    int index_id_for_cc,
    std::unique_ptr<VectorRecord>& data_ptr,
    int64_t now_time_stamp,
    int slot) -> bool {
#ifdef CANDY_ENABLE_METRICS
    uint64_t lock_start = ScopedAccumulateAtomic::now_ns();
#endif
    std::unique_ptr<VectorRecord> data_for_index_insert = nullptr;
    if (use_index_ && concurrency_manager_ && index_id_for_cc != -1) {
        data_for_index_insert = std::make_unique<VectorRecord>(*data_ptr);
    }
#ifdef CANDY_ENABLE_METRICS
    uint64_t before_lock = ScopedAccumulateAtomic::now_ns();
#endif
    std::unique_lock<std::shared_mutex> lock(records_mutex);
#ifdef CANDY_ENABLE_METRICS
    JoinMetrics::instance().lock_wait_ns.fetch_add(ScopedAccumulateAtomic::now_ns() - before_lock, std::memory_order_relaxed);
    ScopedTimerAtomic t_window(JoinMetrics::instance().window_insert_ns);
    if (slot==0) JoinMetrics::instance().total_records_left.fetch_add(1,std::memory_order_relaxed); else JoinMetrics::instance().total_records_right.fetch_add(1,std::memory_order_relaxed);
#endif
    records.emplace_back(std::move(data_ptr));

    if (use_index_ && concurrency_manager_ && data_for_index_insert && index_id_for_cc != -1) {
#ifdef CANDY_ENABLE_METRICS
        {
            ScopedTimerAtomic t_idx(JoinMetrics::instance().index_insert_ns);
            // lock.unlock();
            concurrency_manager_->insert(index_id_for_cc, std::move(data_for_index_insert));
            // lock.lock();
        }
#else
        // 解锁可能会导致竞态，索引内部的插入和删除顺序可能和窗口不一致
        // lock.unlock();
        CANDY_LOG_DEBUG("JOIN", "Inserting to index id={} uid={} ", index_id_for_cc, data_for_index_insert->uid_);
        concurrency_manager_->insert(index_id_for_cc, std::move(data_for_index_insert));
        // lock.lock();
#endif
    }

    auto& window = (slot == 0) ? join_func_->threadSafeWindowL : join_func_->threadSafeWindowR;

    int64_t timelimit = window.windowTimeLimit(now_time_stamp);
#ifdef CANDY_ENABLE_METRICS
    {
        ScopedTimerAtomic t_expire(JoinMetrics::instance().expire_ns);
#endif
      CANDY_LOG_DEBUG("JOIN", "Expiring records before timestamp {} now={} current_size={} ", timelimit, now_time_stamp, records.size());
      try {
        while (!records.empty() && records.front()->timestamp_ <= timelimit) {
          uint64_t expired_uid = records.front()->uid_;
          records.pop_front();
          if (use_index_ && concurrency_manager_ && index_id_for_cc != -1) {
#ifdef CANDY_ENABLE_METRICS
            ScopedTimerAtomic t_idx(JoinMetrics::instance().index_insert_ns);
#endif
            // lock.unlock();
            concurrency_manager_->erase(index_id_for_cc, expired_uid);
            // lock.lock();
          }
        }
        CANDY_LOG_DEBUG("JOIN", "Expiration loop finished. current_size={} ", records.size());
      } catch (const std::exception& e) {
        CANDY_LOG_ERROR("JOIN", "Exception during expiration: what={} ", e.what());
      }
#ifdef CANDY_ENABLE_METRICS
    }
#endif

    CANDY_LOG_DEBUG("JOIN", "Before unlocking records mutex. size={} ", records.size());
    lock.unlock();
    CANDY_LOG_DEBUG("JOIN", "After unlocking records mutex; computing trigger.");
    bool needTrigger = false;
    try {
        needTrigger = window.isNeedTrigger(now_time_stamp);
    } catch (const std::exception& e) {
        CANDY_LOG_ERROR("JOIN", "Exception during isNeedTrigger: what={} ", e.what());
        throw;
    }
    CANDY_LOG_DEBUG("JOIN", "isNeedTrigger={} ", needTrigger ? 1 : 0);
    return needTrigger;
}

// ================== 旧 fallback 接口（仍保留） ==================
auto JoinOperator::lazy_process(const int slot) -> std::optional<Response> {
  std::shared_lock<std::shared_mutex> left_lock(left_records_mutex_);
  std::shared_lock<std::shared_mutex> right_lock(right_records_mutex_);
  if (left_records_.empty() || right_records_.empty()) return std::nullopt;
  left_lock.unlock(); right_lock.unlock();
  std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> local_return_pool;
  // 不再执行旧Excute逻辑，这里直接返回空；保留接口防止外部调用崩溃
  return std::nullopt;
}

auto JoinOperator::eager_process(const int slot) -> std::optional<Response> {
  std::shared_lock<std::shared_mutex> left_lock(left_records_mutex_);
  std::shared_lock<std::shared_mutex> right_lock(right_records_mutex_);
  if ((slot==0 && left_records_.empty()) || (slot==1 && right_records_.empty())) return std::nullopt;
  return std::nullopt;
}

auto JoinOperator::process(Response& input_data, int slot) -> std::optional<Response> {
    if (!input_data.record_) return std::nullopt;
    std::unique_ptr<VectorRecord> data_ptr = std::make_unique<VectorRecord>(*input_data.record_);
    int64_t now_time_stamp = data_ptr->timestamp_;

    bool trigger_flag = (slot == 0)
        ? updateSideThreadSafe(left_records_, left_records_mutex_, left_index_id_, data_ptr, now_time_stamp, slot)
        : updateSideThreadSafe(right_records_, right_records_mutex_, right_index_id_, data_ptr, now_time_stamp, slot);

    if (!trigger_flag) return std::nullopt;

    // 新框架中 process 不直接返回结果（改由 apply / collector 机制），保持兼容返回空
    return std::nullopt;
}

std::vector<std::unique_ptr<VectorRecord>> JoinOperator::getCandidates(
    const std::unique_ptr<VectorRecord>& data_ptr, int slot) {
#ifdef CANDY_ENABLE_METRICS
    ScopedTimerAtomic t_fetch(JoinMetrics::instance().candidate_fetch_ns);
#endif
    if (is_eager_) {
        return join_method_->ExecuteEager(*data_ptr, slot);
    }
    std::deque<std::unique_ptr<VectorRecord>> query_records_copy; // 改为 deque
    if (slot == left_slot_id_) {
        std::shared_lock<std::shared_mutex> lk(left_records_mutex_);
#ifdef CANDY_ENABLE_METRICS
        uint64_t before_wait = ScopedAccumulateAtomic::now_ns();
#endif
        for (auto &p : left_records_)
          if (p) {
            query_records_copy.emplace_back(std::make_unique<VectorRecord>(*p));
          }
    } else {
        std::shared_lock<std::shared_mutex> lk(right_records_mutex_);
        for (auto &p : right_records_)
          if (p) {
            query_records_copy.emplace_back(std::make_unique<VectorRecord>(*p));
          }
    }
    return join_method_->ExecuteLazy(query_records_copy, slot);
}

bool JoinOperator::validateCandidateInWindow(
    const std::unique_ptr<VectorRecord>& candidate,
    const std::deque<std::unique_ptr<VectorRecord>>& window_records) {
    if (!candidate) return false;
    for (auto &r : window_records) if (r && r->uid_ == candidate->uid_) return true;
    return false;
}

void JoinOperator::executeJoinForCandidates(
    const std::vector<std::unique_ptr<VectorRecord>>& candidates,
    const std::unique_ptr<VectorRecord>& data_ptr,
    int slot,
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>>& local_return_pool) {
#ifdef CANDY_ENABLE_METRICS
    ScopedTimerAtomic t_similarity(JoinMetrics::instance().similarity_ns);
#endif
    if (slot == 0) {
        std::shared_lock<std::shared_mutex> rk(right_records_mutex_);
#ifdef CANDY_ENABLE_METRICS
        JoinMetrics::instance().lock_wait_ns.fetch_add(0, std::memory_order_relaxed);
#endif
        for (auto &cand : candidates) {
            if (validateCandidateInWindow(cand, right_records_)) {
                auto left_copy = std::make_unique<VectorRecord>(*data_ptr);
                auto right_copy = std::make_unique<VectorRecord>(*cand);
                uint64_t log_left_uid = left_copy->uid_;
                uint64_t log_right_uid = right_copy->uid_;
                Response lhs{ResponseType::Record, std::move(left_copy)};
                Response rhs{ResponseType::Record, std::move(right_copy)};
#ifdef CANDY_ENABLE_METRICS
                {
                    ScopedTimerAtomic t_joinF(JoinMetrics::instance().join_function_ns);
#endif
                    try {
                        auto res = join_func_->Execute(lhs, rhs);
                        uint64_t result_uid = res.record_ ? res.record_->uid_ : 0;
                        if (res.record_) {
                            local_return_pool.emplace_back(left_slot_id_, std::move(res.record_));
                        }
                        CANDY_LOG_DEBUG("JOIN_EXEC", "slot={} result_uid={} left_uid={} right_uid={} ",
                                       slot, result_uid, log_left_uid, log_right_uid);
                    } catch (const std::exception& e) {
                        CANDY_LOG_ERROR("JOIN_EXEC", "slot={} left_dim={} right_dim={} left_uid={} right_uid={} what={} ",
                                         slot,
                                         (lhs.record_ ? lhs.record_->data_.dim_ : -1),
                                         (rhs.record_ ? rhs.record_->data_.dim_ : -1),
                                         (lhs.record_ ? lhs.record_->uid_ : 0),
                                         (rhs.record_ ? rhs.record_->uid_ : 0),
                                         e.what());
                        throw; // 继续向上抛出以保持现有行为
                    }
#ifdef CANDY_ENABLE_METRICS
                }
#endif
            }
        }
    } else {
        std::shared_lock<std::shared_mutex> lk(left_records_mutex_);
        for (auto &cand : candidates) {
            if (validateCandidateInWindow(cand, left_records_)) {
                auto left_copy = std::make_unique<VectorRecord>(*cand);
                auto right_copy = std::make_unique<VectorRecord>(*data_ptr);
                uint64_t log_left_uid = left_copy->uid_;
                uint64_t log_right_uid = right_copy->uid_;
                Response lhs{ResponseType::Record, std::move(left_copy)};
                Response rhs{ResponseType::Record, std::move(right_copy)};
#ifdef CANDY_ENABLE_METRICS
                {
                    ScopedTimerAtomic t_joinF(JoinMetrics::instance().join_function_ns);
#endif
                try {
                    auto res = join_func_->Execute(lhs, rhs);
                    uint64_t result_uid = res.record_ ? res.record_->uid_ : 0;
                    if (res.record_) {
                        local_return_pool.emplace_back(left_slot_id_, std::move(res.record_));
                    }
                    CANDY_LOG_DEBUG("JOIN_EXEC", "slot={} result_uid={} left_uid={} right_uid={} ",
                                   slot, result_uid, log_left_uid, log_right_uid);
                } catch (const std::exception& e) {
                    CANDY_LOG_ERROR("JOIN_EXEC", "slot={} left_dim={} right_dim={} left_uid={} right_uid={} what={} ",
                                     slot,
                                     (lhs.record_ ? lhs.record_->data_.dim_ : -1),
                                     (rhs.record_ ? rhs.record_->data_.dim_ : -1),
                                     (lhs.record_ ? lhs.record_->uid_ : 0),
                                     (rhs.record_ ? rhs.record_->uid_ : 0),
                                     e.what());
                    throw;
                }
#ifdef CANDY_ENABLE_METRICS
                }
#endif
            }
        }
    }
}

void JoinOperator::executeLazyJoin(
    const std::vector<std::unique_ptr<VectorRecord>>& candidates,
    int slot,
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>>& local_return_pool) {
    if (slot == left_slot_id_) {
        std::shared_lock<std::shared_mutex> rk(right_records_mutex_);
        std::shared_lock<std::shared_mutex> lk(left_records_mutex_);
        for (auto &l : left_records_) {
            if (!l) continue;
            for (auto &cand : candidates) {
                if (validateCandidateInWindow(cand, right_records_)) {
                    auto left_copy = std::make_unique<VectorRecord>(*l);
                    auto right_copy = std::make_unique<VectorRecord>(*cand);
                    Response lhs{ResponseType::Record, std::move(left_copy)};
                    Response rhs{ResponseType::Record, std::move(right_copy)};
                    try {
                        auto res = join_func_->Execute(lhs, rhs);
                        if (res.record_) local_return_pool.emplace_back(left_slot_id_, std::move(res.record_));
                    } catch (const std::exception& e) {
                        CANDY_LOG_ERROR("JOIN_LAZY", "slot={} left_dim={} right_dim={} left_uid={} right_uid={} what={} ",
                                         slot,
                                         (lhs.record_ ? lhs.record_->data_.dim_ : -1),
                                         (rhs.record_ ? rhs.record_->data_.dim_ : -1),
                                         (lhs.record_ ? lhs.record_->uid_ : 0),
                                         (rhs.record_ ? rhs.record_->uid_ : 0),
                                         e.what());
                        throw;
                    }
                }
            }
        }
    } else {
        std::shared_lock<std::shared_mutex> lk(left_records_mutex_);
        std::shared_lock<std::shared_mutex> rk(right_records_mutex_);
        for (auto &r : right_records_) {
            if (!r) continue;
            for (auto &cand : candidates) {
                if (validateCandidateInWindow(cand, left_records_)) {
                    auto left_copy = std::make_unique<VectorRecord>(*cand);
                    auto right_copy = std::make_unique<VectorRecord>(*r);
                    Response lhs{ResponseType::Record, std::move(left_copy)};
                    Response rhs{ResponseType::Record, std::move(right_copy)};
                    try {
                        auto res = join_func_->Execute(lhs, rhs);
                        if (res.record_) local_return_pool.emplace_back(left_slot_id_, std::move(res.record_));
                    } catch (const std::exception& e) {
                        CANDY_LOG_ERROR("JOIN_LAZY", "slot={} left_dim={} right_dim={} left_uid={} right_uid={} what={} ",
                                         slot,
                                         (lhs.record_ ? lhs.record_->data_.dim_ : -1),
                                         (rhs.record_ ? rhs.record_->data_.dim_ : -1),
                                         (lhs.record_ ? lhs.record_->uid_ : 0),
                                         (rhs.record_ ? rhs.record_->uid_ : 0),
                                         e.what());
                        throw;
                    }
                }
            }
        }
    }
}

auto JoinOperator::apply(Response&& record, int slot, Collector& collector) -> void {
#ifdef CANDY_ENABLE_METRICS
    // 统计 apply 处理总耗时（一次调用一次计数）
    JoinMetrics::instance().apply_processing_count.fetch_add(1, std::memory_order_relaxed);
    ScopedTimerAtomic t_apply(JoinMetrics::instance().apply_processing_ns);
    // 记录进入算子的实时时刻，用于端到端延迟统计
    const uint64_t apply_enter_ns = ScopedAccumulateAtomic::now_ns();
#endif
    if (!record.record_) return;
    std::unique_ptr<VectorRecord> data_ptr = std::make_unique<VectorRecord>(*record.record_);
    int64_t now_time_stamp = data_ptr->timestamp_;
    CANDY_LOG_DEBUG("JOIN_APPLY", "Apply called slot={} uid={} ts={} dim={} ", slot, data_ptr->uid_, now_time_stamp, data_ptr->data_.dim_);
    // 重要：为窗口存储拷贝一份，避免 data_ptr 在 updateSideThreadSafe 中被移动导致后续 eager 路径解引用空指针
    auto store_ptr = std::make_unique<VectorRecord>(*data_ptr);
    bool trigger_flag = (slot == left_slot_id_)
        ? updateSideThreadSafe(left_records_, left_records_mutex_, left_index_id_, store_ptr, now_time_stamp, slot)
        : updateSideThreadSafe(right_records_, right_records_mutex_, right_index_id_, store_ptr, now_time_stamp, slot);
    if (!trigger_flag) {
      return;
    }

    auto candidates = getCandidates(data_ptr, slot);
    size_t left_sz = 0, right_sz = 0;
    {
        std::shared_lock<std::shared_mutex> lkL(left_records_mutex_);
        left_sz = left_records_.size();
    }
    {
        std::shared_lock<std::shared_mutex> lkR(right_records_mutex_);
        right_sz = right_records_.size();
    }
    CANDY_LOG_DEBUG("JOIN_APPLY", "slot={} cand={} left_win={} right_win={} eager={} use_index={} ",
           slot, candidates.size(), left_sz, right_sz, (is_eager_?1:0), (use_index_?1:0));
    std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> local_return_pool;

    if (is_eager_) {
        executeJoinForCandidates(candidates, data_ptr, slot, local_return_pool);
    } else {
        executeLazyJoin(candidates, slot, local_return_pool);
        std::unique_lock<std::shared_mutex> lkL(left_records_mutex_);
        std::unique_lock<std::shared_mutex> lkR(right_records_mutex_);
        left_records_.clear();
        right_records_.clear();
    }
#ifdef CANDY_ENABLE_METRICS
    {
        ScopedTimerAtomic t_emit(JoinMetrics::instance().emit_ns);
#endif
    for (auto &p : local_return_pool) {
        Response out{ResponseType::Record, std::move(p.second)};
        collector.collect(std::make_unique<Response>(std::move(out)), p.first);
#ifdef CANDY_ENABLE_METRICS
        JoinMetrics::instance().total_emits.fetch_add(1,std::memory_order_relaxed);
        // 端到端延迟：从 apply 进入到对应结果发射的时长（按每条结果计）
        const uint64_t now_ns = ScopedAccumulateAtomic::now_ns();
        JoinMetrics::instance().e2e_latency_ns.fetch_add(now_ns - apply_enter_ns, std::memory_order_relaxed);
        JoinMetrics::instance().e2e_latency_count.fetch_add(1, std::memory_order_relaxed);
#endif
    }
#ifdef CANDY_ENABLE_METRICS
    }
#endif
}

} // namespace candy
