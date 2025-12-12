#include "metrics/join_metrics_collector.h"

#include <utility>

#include "utils/logger.h"

namespace sageFlow {
namespace metrics {

// ============================================================================
// JoinMetricsCollector Implementation
// ============================================================================

JoinMetricsCollector::JoinMetricsCollector(std::string name) : name_(std::move(name)) {}

void JoinMetricsCollector::startTimer(const std::string& phase) {
    std::lock_guard<std::mutex> lock(mutex_);
    active_timers_[phase] = Clock::now();
}

void JoinMetricsCollector::stopTimer(const std::string& phase) {
    auto end_time = Clock::now();

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = active_timers_.find(phase);
    if (it == active_timers_.end()) {
        SAGEFLOW_LOG_WARN("JoinMetrics", "Timer '{}' was not started for collector '{}'", phase, name_);
        return;
    }

    auto duration =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - it->second);
    active_timers_.erase(it);

    // 累加到对应指标
    if (phase == "total") {
        stats_.total_time += duration;
    } else if (phase == "index_build") {
        stats_.index_build_time += duration;
    } else if (phase == "query") {
        stats_.query_time += duration;
    } else if (phase == "eviction" || phase == "window_eviction") {
        stats_.window_eviction_time += duration;
    } else if (phase == "similarity") {
        stats_.similarity_time += duration;
    } else if (phase == "join_function") {
        stats_.join_function_time += duration;
    } else if (phase == "emit") {
        stats_.emit_time += duration;
    } else if (phase == "lock_wait") {
        stats_.lock_wait_time += duration;
    } else {
        SAGEFLOW_LOG_DEBUG("JoinMetrics", "Unknown timer phase '{}' for collector '{}'", phase, name_);
    }
}

// ============================================================================
// ScopedTimer Implementation
// ============================================================================

JoinMetricsCollector::ScopedTimer::ScopedTimer(JoinMetricsCollector& collector, std::string phase)
    : collector_(&collector), phase_(std::move(phase)), active_(true) {
    collector_->startTimer(phase_);
}

JoinMetricsCollector::ScopedTimer::~ScopedTimer() {
    if (active_ && collector_ != nullptr) {
        collector_->stopTimer(phase_);
    }
}

JoinMetricsCollector::ScopedTimer::ScopedTimer(ScopedTimer&& other) noexcept
    : collector_(other.collector_), phase_(std::move(other.phase_)), active_(other.active_) {
    other.active_ = false;
    other.collector_ = nullptr;
}

JoinMetricsCollector::ScopedTimer& JoinMetricsCollector::ScopedTimer::operator=(ScopedTimer&& other) noexcept {
    if (this != &other) {
        // 如果当前对象活跃，先停止计时
        if (active_ && collector_ != nullptr) {
            collector_->stopTimer(phase_);
        }
        collector_ = other.collector_;
        phase_ = std::move(other.phase_);
        active_ = other.active_;
        other.active_ = false;
        other.collector_ = nullptr;
    }
    return *this;
}

// ============================================================================
// Counter Methods
// ============================================================================

void JoinMetricsCollector::recordLeftProcessed(int64_t count) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.left_records_processed += count;
}

void JoinMetricsCollector::recordRightProcessed(int64_t count) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.right_records_processed += count;
}

void JoinMetricsCollector::recordComparison(int64_t count) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.total_comparisons += count;
}

void JoinMetricsCollector::recordCandidate(int64_t count) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.candidate_pairs += count;
}

void JoinMetricsCollector::recordMatch(int64_t count) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.output_matches += count;
}

void JoinMetricsCollector::recordIndexInsert(int64_t count) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.index_inserts += count;
}

void JoinMetricsCollector::recordIndexDelete(int64_t count) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.index_deletes += count;
}

void JoinMetricsCollector::recordIndexQuery(int64_t count) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.index_queries += count;
}

void JoinMetricsCollector::recordIndexRebuild() {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.index_rebuilds += 1;
}

// ============================================================================
// Accuracy Methods
// ============================================================================

void JoinMetricsCollector::updateAccuracyMetrics(int64_t tp, int64_t fp, int64_t fn) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.true_positives = tp;
    stats_.false_positives = fp;
    stats_.false_negatives = fn;
}

void JoinMetricsCollector::recordTruePositive(int64_t count) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.true_positives += count;
}

void JoinMetricsCollector::recordFalsePositive(int64_t count) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.false_positives += count;
}

void JoinMetricsCollector::recordFalseNegative(int64_t count) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.false_negatives += count;
}

// ============================================================================
// Snapshot Methods
// ============================================================================

JoinExecutionStats JoinMetricsCollector::snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stats_;
}

JoinExecutionStats JoinMetricsCollector::snapshotFromGlobal() {
    JoinExecutionStats stats;
    auto& global = JoinMetrics::instance();

    // 时间指标
    stats.total_time = std::chrono::nanoseconds(global.apply_processing_ns.load(std::memory_order_relaxed));
    stats.index_build_time = std::chrono::nanoseconds(global.index_insert_ns.load(std::memory_order_relaxed));
    stats.query_time = std::chrono::nanoseconds(global.candidate_fetch_ns.load(std::memory_order_relaxed));
    stats.window_eviction_time = std::chrono::nanoseconds(global.expire_ns.load(std::memory_order_relaxed));
    stats.similarity_time = std::chrono::nanoseconds(global.similarity_ns.load(std::memory_order_relaxed));
    stats.join_function_time = std::chrono::nanoseconds(global.join_function_ns.load(std::memory_order_relaxed));
    stats.emit_time = std::chrono::nanoseconds(global.emit_ns.load(std::memory_order_relaxed));
    stats.lock_wait_time = std::chrono::nanoseconds(global.lock_wait_ns.load(std::memory_order_relaxed));

    // 数据规模
    stats.left_records_processed = static_cast<int64_t>(global.total_records_left.load(std::memory_order_relaxed));
    stats.right_records_processed = static_cast<int64_t>(global.total_records_right.load(std::memory_order_relaxed));

    // 匹配统计
    stats.output_matches = static_cast<int64_t>(global.total_emits.load(std::memory_order_relaxed));

    return stats;
}

void JoinMetricsCollector::reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_ = JoinExecutionStats{};
    active_timers_.clear();
}

// ============================================================================
// JoinMetricsRegistry Implementation
// ============================================================================

JoinMetricsRegistry& JoinMetricsRegistry::instance() {
    static JoinMetricsRegistry registry;
    return registry;
}

std::shared_ptr<JoinMetricsCollector> JoinMetricsRegistry::getOrCreate(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = collectors_.find(name);
    if (it != collectors_.end()) {
        return it->second;
    }

    auto collector = std::make_shared<JoinMetricsCollector>(name);
    collectors_[name] = collector;
    return collector;
}

std::shared_ptr<JoinMetricsCollector> JoinMetricsRegistry::get(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = collectors_.find(name);
    if (it != collectors_.end()) {
        return it->second;
    }
    return nullptr;
}

std::vector<std::string> JoinMetricsRegistry::getCollectorNames() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<std::string> names;
    names.reserve(collectors_.size());
    for (const auto& [name, collector] : collectors_) {
        names.push_back(name);
    }
    return names;
}

std::unordered_map<std::string, JoinExecutionStats> JoinMetricsRegistry::allSnapshots() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::unordered_map<std::string, JoinExecutionStats> snapshots;
    for (const auto& [name, collector] : collectors_) {
        snapshots[name] = collector->snapshot();
    }
    return snapshots;
}

void JoinMetricsRegistry::resetAll() {
    std::lock_guard<std::mutex> lock(mutex_);

    for (auto& [name, collector] : collectors_) {
        collector->reset();
    }
}

bool JoinMetricsRegistry::remove(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    return collectors_.erase(name) > 0;
}

void JoinMetricsRegistry::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    collectors_.clear();
}

}  // namespace metrics
}  // namespace sageFlow
