#include "operator/join_operator_methods/s3j_components/adaptive_index_selector.h"

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace sageFlow {

AdaptiveIndexSelector::AdaptiveIndexSelector(const AdaptiveIndexSelectorConfig& config)
    : config_(config) {}

IndexType AdaptiveIndexSelector::selectBestIndex(int dimension, 
                                                   size_t data_size, 
                                                   double query_rate) const {
    // 基于数据规模的启发式选择
    
    // 小数据集：使用 BruteForce（精确但慢）
    if (data_size <= thresholds_.bruteforce_max) {
        return IndexType::BruteForce;
    }
    
    // 中等数据集：使用 IVF
    if (data_size <= thresholds_.ivf_preferred_max) {
        return IndexType::IVF;
    }
    
    // 大数据集：使用 HNSW
    return IndexType::HNSW;
}

IndexType AdaptiveIndexSelector::shouldSwitchIndex(IndexType current_type,
                                                     const IndexPerformance& current_perf,
                                                     size_t data_size,
                                                     int dimension) const {
    if (!config_.enable_auto_switch) {
        return current_type;
    }
    
    // 如果当前性能不可用，维持现状
    if (!current_perf.isValid()) {
        return current_type;
    }
    
    // 估算其他索引类型的理论性能
    std::vector<IndexType> candidates = {IndexType::BruteForce, IndexType::IVF, IndexType::HNSW};
    
    IndexType best_type = current_type;
    double best_score = current_perf.avg_latency_us;
    
    for (IndexType type : candidates) {
        if (type == current_type) continue;
        
        // 检查缓存的性能数据
        auto cached = getCachedPerformance(type);
        double estimated_latency;
        
        if (cached.isValid()) {
            // 使用实际缓存数据
            estimated_latency = cached.avg_latency_us;
        } else {
            // 使用理论估算
            auto estimated = estimatePerformance(type, dimension, data_size);
            estimated_latency = estimated.avg_latency_us;
        }
        
        // 检查是否有显著改进
        double improvement = (best_score - estimated_latency) / best_score;
        if (improvement > config_.switch_threshold) {
            best_type = type;
            best_score = estimated_latency;
        }
    }
    
    return best_type;
}

void AdaptiveIndexSelector::updatePerformanceCache(IndexType type, const IndexPerformance& perf) {
    perf_cache_[type] = perf;
}

std::map<std::string, std::string> AdaptiveIndexSelector::getRecommendedParams(
    IndexType type, int dimension, size_t expected_size) const {
    
    std::map<std::string, std::string> params;
    
    switch (type) {
        case IndexType::BruteForce:
            // BruteForce 无特殊参数
            break;
            
        case IndexType::IVF: {
            // nlist: 聚类数量，通常为 sqrt(n)
            int nlist = std::max(10, static_cast<int>(std::sqrt(expected_size)));
            nlist = std::min(nlist, 1000);  // 上限
            params["nlist"] = std::to_string(nlist);
            
            // nprobes: 查询时探测的聚类数
            int nprobes = std::max(1, nlist / 10);
            nprobes = std::min(nprobes, 50);
            params["nprobes"] = std::to_string(nprobes);
            break;
        }
        
        case IndexType::HNSW: {
            // M: 每层最大邻居数
            int m = 16;
            if (expected_size > 100000) m = 32;
            if (expected_size > 1000000) m = 48;
            params["M"] = std::to_string(m);
            
            // ef_construction: 构建时的候选集大小
            int ef_construction = std::max(m * 2, 100);
            params["ef_construction"] = std::to_string(ef_construction);
            
            // ef_search: 查询时的候选集大小
            int ef_search = std::max(m, 50);
            params["ef_search"] = std::to_string(ef_search);
            break;
        }
        
        default:
            break;
    }
    
    return params;
}

IndexPerformance AdaptiveIndexSelector::getCachedPerformance(IndexType type) const {
    auto it = perf_cache_.find(type);
    if (it != perf_cache_.end()) {
        return it->second;
    }
    return IndexPerformance();  // 返回空的性能数据
}

void AdaptiveIndexSelector::clearCache() {
    perf_cache_.clear();
}

std::string AdaptiveIndexSelector::indexTypeToString(IndexType type) {
    switch (type) {
        case IndexType::None: return "None";
        case IndexType::BruteForce: return "BruteForce";
        case IndexType::IVF: return "IVF";
        case IndexType::HNSW: return "HNSW";
        case IndexType::Vectraflow: return "Vectraflow";
        case IndexType::PartitionedIndex: return "PartitionedIndex";
        default: return "Unknown";
    }
}

IndexType AdaptiveIndexSelector::stringToIndexType(const std::string& str) {
    if (str == "None") return IndexType::None;
    if (str == "BruteForce") return IndexType::BruteForce;
    if (str == "IVF") return IndexType::IVF;
    if (str == "HNSW") return IndexType::HNSW;
    if (str == "Vectraflow") return IndexType::Vectraflow;
    if (str == "PartitionedIndex") return IndexType::PartitionedIndex;
    return IndexType::None;
}

IndexPerformance AdaptiveIndexSelector::estimatePerformance(IndexType type, 
                                                             int dimension, 
                                                             size_t data_size) const {
    IndexPerformance perf;
    perf.sample_count = 1;  // 标记为有效
    
    // 基于复杂度的理论延迟估算（微秒）
    double base_ops = static_cast<double>(dimension);  // 基本操作：向量维度相关
    
    switch (type) {
        case IndexType::BruteForce:
            // O(n * d) - 线性扫描
            perf.avg_latency_us = base_ops * data_size * 0.001;  // 调整因子
            perf.recall = 1.0;  // 精确
            perf.memory_mb = static_cast<double>(data_size * dimension * sizeof(float)) / (1024 * 1024);
            break;
            
        case IndexType::IVF: {
            // O(sqrt(n) * d) - 聚类后搜索
            double clusters = std::sqrt(static_cast<double>(data_size));
            double probes = clusters * 0.1;  // 假设探测 10% 的聚类
            perf.avg_latency_us = base_ops * (clusters + data_size / clusters * probes) * 0.001;
            perf.recall = 0.95;  // 近似
            perf.memory_mb = static_cast<double>(data_size * dimension * sizeof(float) * 1.1) / (1024 * 1024);
            break;
        }
        
        case IndexType::HNSW:
            // O(log(n) * d * M) - 图搜索
            perf.avg_latency_us = base_ops * std::log(data_size + 1) * 16 * 0.01;  // M=16
            perf.recall = 0.98;  // 高召回近似
            perf.memory_mb = static_cast<double>(data_size * (dimension * sizeof(float) + 16 * 2 * sizeof(int))) / (1024 * 1024);
            break;
            
        default:
            perf.avg_latency_us = base_ops * data_size * 0.001;
            perf.recall = 1.0;
            break;
    }
    
    return perf;
}

}  // namespace sageFlow
