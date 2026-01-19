#include "operator/join_operator_methods/s3j_components/adaptive_partitioner.h"

#include <algorithm>
#include <cmath>
#include <queue>
#include <numeric>
#include <unordered_set>
#include <sstream>
#include <map>

namespace sageFlow {

AdaptivePartitioner::AdaptivePartitioner(int dimension,
                                          const AdaptivePartitionerConfig& config,
                                          int seed)
    : KMeansPartitioner(dimension, config.initial_partitions, seed),
      adapt_config_(config) {}

// [S3J Paper] Algorithm 1: Workset Balancing Algorithm implementation
std::vector<MigrationPlan> AdaptivePartitioner::runGreedyBalancing(
    const std::vector<WorksetLoadInfo>& all_worksets,
    int num_workers) {
    
    std::vector<MigrationPlan> plans;
    if (num_workers <= 1 || all_worksets.empty()) return plans;

    // 1. 重建各 Worker 的当前负载状态
    std::vector<double> worker_loads(num_workers, 0.0);
    // 同时也需要按 Worker 索引 Workset，方便后续遍历
    std::vector<std::vector<const WorksetLoadInfo*>> worker_worksets(num_workers);

    double total_load = 0.0;
    for (const auto& w : all_worksets) {
        if (w.worker_id >= 0 && w.worker_id < num_workers) {
            worker_loads[w.worker_id] += w.load;
            worker_worksets[w.worker_id].push_back(&w);
            total_load += w.load;
        }
    }

    double avg_load = total_load / num_workers;
    if (avg_load < 1e-6) return plans; // 负载过低无需平衡

    // 检查当前不平衡度，如果低于阈值则跳过
    double current_di = computeImbalance(worker_loads, avg_load);
    if ((current_di / avg_load) <= adapt_config_.load_threshold) {
        return plans; // [Optimization] 无需调整
    }

    // 2. 区分 Overloaded (O) 和 Underloaded (U) 集合
    std::vector<int> O_workers; // Overloaded
    std::vector<int> U_workers; // Underloaded

    for (int i = 0; i < num_workers; ++i) {
        if (worker_loads[i] > avg_load) O_workers.push_back(i);
        else U_workers.push_back(i);
    }

    // 优先队列元素: {benefit, workset_ptr}
    struct Candidate {
        double benefit;
        const WorksetLoadInfo* workset;
        
        bool operator<(const Candidate& other) const {
            return benefit < other.benefit; // Max heap
        }
    };

    std::priority_queue<Candidate> over_benefits;
    std::unordered_set<uint64_t> ignore_list; // 已处理或不可移动的 Workset

    // 3. 初始化候选移动 (Lines 6-13)
    // 遍历所有过载 Worker 的 Workset
    for (int worker_idx : O_workers) {
        for (const auto* w : worker_worksets[worker_idx]) {
            // [Paper Line 5] Find Irremovables
            // "flag big worksets with load higher than average load... as irremovable" 
            if (w->load > avg_load) {
                ignore_list.insert(w->workset_id);
                continue;
            }

            double benefit = calculateRemovalBenefit(*w, worker_loads[worker_idx], avg_load);
            if (benefit > 0) {
                over_benefits.push({benefit, w});
            }
        }
    }

    // 模拟状态，防止同一个 Worker 被过度掏空或填满
    std::vector<double> simulated_loads = worker_loads;

    // 4. 贪心分配 (Lines 14-32)
    while (!over_benefits.empty()) {
        Candidate best = over_benefits.top();
        over_benefits.pop();

        if (ignore_list.count(best.workset->workset_id)) continue;

        int best_target = -1;
        double max_addition_benefit = -std::numeric_limits<double>::infinity();

        // 在所有空闲节点中寻找最佳归宿 (Lines 17-21)
        for (int u_idx : U_workers) {
            // 计算如果把 workset 加到这个 worker 带来的收益
            double benefit = calculateAdditionBenefit(*best.workset, simulated_loads[u_idx], avg_load);
            
            // [Algorithm Constraint] 确保移动后目标节点不会瞬间变得比源节点还过载
            // Paper Line 24: "until compute_load(optimal, u) < Lavg" 这里的条件稍显模糊，
            // 我们采用更稳健的逻辑：移动后目标负载最好不超过 avg_load * 1.05 (容忍度)
            // 或者单纯保证 benefit > 0 且最大化 benefit 即可。
            
            if (benefit > max_addition_benefit) {
                // 检查移动后的目标负载是否会过度
                if (simulated_loads[u_idx] + best.workset->load < avg_load * 1.1) {
                     max_addition_benefit = benefit;
                     best_target = u_idx;
                }
            }
        }

        if (best_target != -1) {
            // 生成迁移计划
            plans.push_back({best.workset->workset_id, best.workset->worker_id, best_target});
            
            // 更新模拟负载
            simulated_loads[best.workset->worker_id] -= best.workset->load;
            simulated_loads[best_target] += best.workset->load;
            
            // 记录日志
            std::ostringstream ss;
            ss << "Rebalance WS-" << best.workset->workset_id << " (" << best.workset->load 
               << ") from W-" << best.workset->worker_id << " to W-" << best_target;
            recordHistory("rebalance", best.workset->workset_id, ss.str());
            
            // 将该 Workset 加入忽略列表，防止重复移动
            ignore_list.insert(best.workset->workset_id);
            
            // 论文中是一个贪心循环，实际上源节点的负载变了，其他 workset 的 removal_benefit 也会变。
            // 为了简化计算复杂度，我们通常在一次调度周期内不重新计算所有 benefit，
            // 而是依赖下一次 adapt_interval 的微调。
        } else {
            // 无法找到合适的目标，加入忽略列表
            ignore_list.insert(best.workset->workset_id);
        }
    }

    return plans;
}

// Benefit = (Old DI contribution) - (New DI contribution) - Migration Cost
double AdaptivePartitioner::calculateRemovalBenefit(
    const WorksetLoadInfo& w, double src_load, double avg_load) const {
    
    // 当前该 Worker 对 DI 的贡献: |L - Avg|
    double current_imbalance = std::abs(src_load - avg_load);
    // 移除后的贡献: |(L - w) - Avg|
    double new_imbalance = std::abs((src_load - w.load) - avg_load);
    
    double imbalance_reduction = current_imbalance - new_imbalance;
    
    // 迁移成本 = Size * Factor
    double cost = w.size_bytes * adapt_config_.migration_factor;
    
    return imbalance_reduction - cost;
}

// Addition Benefit = (Old DI contribution) - (New DI contribution)
double AdaptivePartitioner::calculateAdditionBenefit(
    const WorksetLoadInfo& w, double target_load, double avg_load) const {
    
    double current_imbalance = std::abs(target_load - avg_load);
    double new_imbalance = std::abs((target_load + w.load) - avg_load);
    
    return current_imbalance - new_imbalance;
}

double AdaptivePartitioner::computeImbalance(const std::vector<double>& worker_loads, double avg_load) const {
    double di = 0.0;
    for (double load : worker_loads) {
        di += std::abs(load - avg_load);
    }
    return di;
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
    
    // // 检查是否需要分裂
    // if (num_partitions < adapt_config_.max_partitions &&
    //     max_partition < partition_stats_.size()) {
    //     double max_load = static_cast<double>(partition_stats_[max_partition].count.load());
    //     if (max_load > avg_load * adapt_config_.split_threshold) {
    //         if (splitPartition(max_partition)) {
    //             adapted = true;
    //         }
    //     }
    // }
    
    // // 检查是否需要合并
    // if (!adapted && num_partitions > adapt_config_.min_partitions &&
    //     min_partition < partition_stats_.size()) {
    //     double min_load = static_cast<double>(partition_stats_[min_partition].count.load());
    //     if (min_load < avg_load * adapt_config_.merge_threshold) {
    //         size_t neighbor = findNeighborPartition(min_partition);
    //         if (neighbor != min_partition && mergePartitions(min_partition, neighbor)) {
    //             adapted = true;
    //         }
    //     }
    // }
    
    return false;
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

// bool AdaptivePartitioner::splitPartition(size_t partition) {
//     // 注意：调用此函数时应持有 stats_mutex_
//     int num_partitions = current_num_partitions_.load();
    
//     if (num_partitions >= adapt_config_.max_partitions) {
//         return false;
//     }
    
//     // 增加分区数
//     current_num_partitions_.fetch_add(1);
    
//     // 扩展统计数组
//     partition_stats_.resize(num_partitions + 1);
    
//     // 重置被分裂分区和新分区的统计
//     if (partition < partition_stats_.size()) {
//         partition_stats_[partition].reset();
//     }
//     partition_stats_[num_partitions].reset();
    
//     // 记录历史
//     std::ostringstream ss;
//     ss << "Split partition " << partition << " into " << partition << " and " << num_partitions;
//     recordHistory("split", static_cast<int>(partition), ss.str());
    
//     return true;
// }

// bool AdaptivePartitioner::mergePartitions(size_t partition1, size_t partition2) {
//     // 注意：调用此函数时应持有 stats_mutex_
//     int num_partitions = current_num_partitions_.load();
    
//     if (num_partitions <= adapt_config_.min_partitions) {
//         return false;
//     }
    
//     if (partition1 >= static_cast<size_t>(num_partitions) || 
//         partition2 >= static_cast<size_t>(num_partitions)) {
//         return false;
//     }
    
//     // 减少分区数
//     current_num_partitions_.fetch_sub(1);
    
//     // 合并统计到 partition1
//     if (partition1 < partition_stats_.size() && partition2 < partition_stats_.size()) {
//         partition_stats_[partition1].count.fetch_add(
//             partition_stats_[partition2].count.load());
//         partition_stats_[partition1].total_latency_us.fetch_add(
//             partition_stats_[partition2].total_latency_us.load());
//         partition_stats_[partition1].data_size.fetch_add(
//             partition_stats_[partition2].data_size.load());
//         partition_stats_[partition2].reset();
//     }
    
//     // 记录历史
//     std::ostringstream ss;
//     ss << "Merged partitions " << partition1 << " and " << partition2;
//     recordHistory("merge", static_cast<int>(partition1), ss.str());
    
//     return true;
// }

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
                                          int id, 
                                          const std::string& details) {
    std::lock_guard<std::mutex> lock(history_mutex_);
    AdaptHistory entry;
    entry.timestamp = std::chrono::steady_clock::now();
    entry.action = action;
    entry.partition_id = id;
    entry.details = details;
    history_.push_back(std::move(entry));
    if (history_.size() > kMaxHistorySize) history_.erase(history_.begin());
}

int64_t AdaptivePartitioner::getCurrentTimeMs() {
    auto now = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    return ms.count();
}

}  // namespace sageFlow
