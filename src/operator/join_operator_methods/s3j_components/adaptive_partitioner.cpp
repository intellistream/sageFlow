#include "operator/join_operator_methods/s3j_components/adaptive_partitioner.h"

#include <algorithm>
#include <cmath>
#include <numeric>
#include <sstream>

namespace sageFlow {

AdaptivePartitioner::AdaptivePartitioner(int dimension,
                                          const AdaptivePartitionerConfig& config,
                                          int seed)
    : KMeansPartitioner(dimension, config.initial_partitions, seed),
      adapt_config_(config),
      last_adapt_time_ms_(getCurrentTimeMs()),
      current_num_partitions_(config.initial_partitions) {
    
    // 初始化分区统计
    partition_stats_ = std::vector<PartitionStats>(config.initial_partitions);
}

void AdaptivePartitioner::updateStats(size_t partition, int64_t latency_us, size_t data_size) {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    
    size_t num_partitions = static_cast<size_t>(current_num_partitions_.load());
    if (partition >= num_partitions) {
        return;  // 无效分区
    }
    
    // 确保 partition_stats_ 大小足够
    if (partition >= partition_stats_.size()) {
        size_t old_size = partition_stats_.size();
        size_t new_size = partition + 1;
        // 直接扩展 vector，添加默认构造的元素
        partition_stats_.resize(new_size);
    }
    
    partition_stats_[partition].count.fetch_add(1, std::memory_order_relaxed);
    partition_stats_[partition].total_latency_us.fetch_add(latency_us, std::memory_order_relaxed);
    partition_stats_[partition].data_size.fetch_add(data_size, std::memory_order_relaxed);
}

bool AdaptivePartitioner::checkAndAdapt() {
    int64_t now = getCurrentTimeMs();
    int64_t last = last_adapt_time_ms_.load(std::memory_order_relaxed);
    
    // 检查时间间隔
    if (now - last < adapt_config_.adapt_interval_ms) {
        return false;
    }
    
    // CAS 更新时间戳，避免并发调整
    if (!last_adapt_time_ms_.compare_exchange_strong(last, now)) {
        return false;
    }
    
    return forceAdapt();
}

bool AdaptivePartitioner::forceAdapt() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    
    int num_partitions = current_num_partitions_.load();
    if (num_partitions <= 1) {
        return false;
    }
    
    // 计算负载不均衡度
    double imbalance = computeLoadImbalance();
    
    if (imbalance <= adapt_config_.load_threshold) {
        // 负载均衡，无需调整
        return false;
    }
    
    bool adapted = false;
    
    // 找到最大和最小负载分区
    size_t max_partition = findMaxLoadPartition();
    size_t min_partition = findMinLoadPartition();
    
    // 计算平均负载
    double total_load = 0.0;
    for (size_t i = 0; i < static_cast<size_t>(num_partitions); ++i) {
        if (i < partition_stats_.size()) {
            total_load += static_cast<double>(partition_stats_[i].count.load());
        }
    }
    double avg_load = total_load / num_partitions;
    
    // 检查是否需要分裂
    if (num_partitions < adapt_config_.max_partitions &&
        max_partition < partition_stats_.size()) {
        double max_load = static_cast<double>(partition_stats_[max_partition].count.load());
        if (max_load > avg_load * adapt_config_.split_threshold) {
            if (splitPartition(max_partition)) {
                adapted = true;
            }
        }
    }
    
    // 检查是否需要合并
    if (!adapted && num_partitions > adapt_config_.min_partitions &&
        min_partition < partition_stats_.size()) {
        double min_load = static_cast<double>(partition_stats_[min_partition].count.load());
        if (min_load < avg_load * adapt_config_.merge_threshold) {
            size_t neighbor = findNeighborPartition(min_partition);
            if (neighbor != min_partition && mergePartitions(min_partition, neighbor)) {
                adapted = true;
            }
        }
    }
    
    return adapted;
}

int AdaptivePartitioner::getCurrentNumPartitions() const {
    return current_num_partitions_.load();
}

std::vector<AdaptHistory> AdaptivePartitioner::getHistory() const {
    std::lock_guard<std::mutex> lock(history_mutex_);
    return history_;
}

void AdaptivePartitioner::clearHistory() {
    std::lock_guard<std::mutex> lock(history_mutex_);
    history_.clear();
}

double AdaptivePartitioner::computeLoadImbalance() const {
    // 注意：调用此函数时应持有 stats_mutex_
    int num_partitions = current_num_partitions_.load();
    if (num_partitions <= 1) {
        return 0.0;
    }
    
    double total = 0.0;
    double max_load = 0.0;
    
    for (size_t i = 0; i < static_cast<size_t>(num_partitions) && i < partition_stats_.size(); ++i) {
        double load = static_cast<double>(partition_stats_[i].count.load());
        total += load;
        max_load = std::max(max_load, load);
    }
    
    double avg = total / num_partitions;
    if (avg <= 0) {
        return 0.0;
    }
    
    return (max_load / avg) - 1.0;
}

const PartitionStats& AdaptivePartitioner::getPartitionStats(size_t partition) const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    static PartitionStats empty_stats;
    if (partition >= partition_stats_.size()) {
        return empty_stats;
    }
    return partition_stats_[partition];
}

void AdaptivePartitioner::resetStats() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    for (auto& stats : partition_stats_) {
        stats.reset();
    }
}

bool AdaptivePartitioner::splitPartition(size_t partition) {
    // 注意：调用此函数时应持有 stats_mutex_
    int num_partitions = current_num_partitions_.load();
    
    if (num_partitions >= adapt_config_.max_partitions) {
        return false;
    }
    
    // 增加分区数
    current_num_partitions_.fetch_add(1);
    
    // 扩展统计数组
    partition_stats_.resize(num_partitions + 1);
    
    // 重置被分裂分区和新分区的统计
    if (partition < partition_stats_.size()) {
        partition_stats_[partition].reset();
    }
    partition_stats_[num_partitions].reset();
    
    // 记录历史
    std::ostringstream ss;
    ss << "Split partition " << partition << " into " << partition << " and " << num_partitions;
    recordHistory("split", static_cast<int>(partition), ss.str());
    
    return true;
}

bool AdaptivePartitioner::mergePartitions(size_t partition1, size_t partition2) {
    // 注意：调用此函数时应持有 stats_mutex_
    int num_partitions = current_num_partitions_.load();
    
    if (num_partitions <= adapt_config_.min_partitions) {
        return false;
    }
    
    if (partition1 >= static_cast<size_t>(num_partitions) || 
        partition2 >= static_cast<size_t>(num_partitions)) {
        return false;
    }
    
    // 减少分区数
    current_num_partitions_.fetch_sub(1);
    
    // 合并统计到 partition1
    if (partition1 < partition_stats_.size() && partition2 < partition_stats_.size()) {
        partition_stats_[partition1].count.fetch_add(
            partition_stats_[partition2].count.load());
        partition_stats_[partition1].total_latency_us.fetch_add(
            partition_stats_[partition2].total_latency_us.load());
        partition_stats_[partition1].data_size.fetch_add(
            partition_stats_[partition2].data_size.load());
        partition_stats_[partition2].reset();
    }
    
    // 记录历史
    std::ostringstream ss;
    ss << "Merged partitions " << partition1 << " and " << partition2;
    recordHistory("merge", static_cast<int>(partition1), ss.str());
    
    return true;
}

size_t AdaptivePartitioner::findMaxLoadPartition() const {
    // 注意：调用此函数时应持有 stats_mutex_
    size_t max_partition = 0;
    size_t max_count = 0;
    
    size_t num_partitions = static_cast<size_t>(current_num_partitions_.load());
    for (size_t i = 0; i < num_partitions && i < partition_stats_.size(); ++i) {
        size_t count = partition_stats_[i].count.load();
        if (count > max_count) {
            max_count = count;
            max_partition = i;
        }
    }
    
    return max_partition;
}

size_t AdaptivePartitioner::findMinLoadPartition() const {
    // 注意：调用此函数时应持有 stats_mutex_
    size_t min_partition = 0;
    size_t min_count = std::numeric_limits<size_t>::max();
    
    size_t num_partitions = static_cast<size_t>(current_num_partitions_.load());
    for (size_t i = 0; i < num_partitions && i < partition_stats_.size(); ++i) {
        size_t count = partition_stats_[i].count.load();
        if (count < min_count) {
            min_count = count;
            min_partition = i;
        }
    }
    
    return min_partition;
}

size_t AdaptivePartitioner::findNeighborPartition(size_t partition) const {
    // 注意：调用此函数时应持有 stats_mutex_
    size_t num_partitions = static_cast<size_t>(current_num_partitions_.load());
    
    if (num_partitions <= 1) {
        return partition;
    }
    
    // 简单策略：选择相邻分区中负载较小的
    size_t left = (partition > 0) ? partition - 1 : num_partitions - 1;
    size_t right = (partition + 1) % num_partitions;
    
    size_t left_count = (left < partition_stats_.size()) 
        ? partition_stats_[left].count.load() : 0;
    size_t right_count = (right < partition_stats_.size()) 
        ? partition_stats_[right].count.load() : 0;
    
    return (left_count <= right_count) ? left : right;
}

void AdaptivePartitioner::recordHistory(const std::string& action, 
                                          int partition_id, 
                                          const std::string& details) {
    std::lock_guard<std::mutex> lock(history_mutex_);
    
    AdaptHistory entry;
    entry.timestamp = std::chrono::steady_clock::now();
    entry.action = action;
    entry.partition_id = partition_id;
    entry.details = details;
    
    history_.push_back(std::move(entry));
    
    // 限制历史大小
    if (history_.size() > kMaxHistorySize) {
        history_.erase(history_.begin());
    }
}

int64_t AdaptivePartitioner::getCurrentTimeMs() {
    auto now = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    return ms.count();
}

}  // namespace sageFlow
