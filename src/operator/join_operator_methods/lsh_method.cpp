#include "operator/join_operator_methods/lsh_method.h"

#include <cmath>
#include <algorithm>
#include <cstring>
#include <unordered_set>
#include <unordered_map>
#include <mutex>
#include <numeric>

#include "operator/utils/join_method_registry.h"
#include "utils/logger.h"

namespace sageFlow {
namespace {
float dotProduct(const std::vector<float>& a, const std::vector<float>& b) {
    float sum = 0.0f;
    const size_t n = std::min(a.size(), b.size());
    for (size_t i = 0; i < n; ++i) {
        sum += a[i] * b[i];
    }
    return sum;
}

// 与全局 E2E 评测保持一致：exp(-alpha * L2)
inline double l2Similarity(const std::vector<float>& a,
                           const std::vector<float>& b) {
    if (a.empty() || b.empty() || a.size() != b.size()) {
        return 0.0;
    }
    double distance_sq = 0.0;
    for (size_t i = 0; i < a.size(); ++i) {
        const double diff = static_cast<double>(a[i]) - static_cast<double>(b[i]);
        distance_sq += diff * diff;
    }
    constexpr double kAlpha = 0.1;  // 与 BruteForceBaseline 一致
    const double distance = std::sqrt(distance_sq);
    return std::exp(-kAlpha * distance);
}
}

LSHMethod::LSHMethod(const Config& config)
    : BaseMethod(config.similarity_threshold), config_(config) {
    // 参数校验与截断
    if (config_.num_tables <= 0) {
        SAGEFLOW_LOG_WARN("LSHMethod", "表数量 num_tables={} 非法，使用默认值 4", config_.num_tables);
        config_.num_tables = 4;
    }
    if (config_.num_hashes <= 0) {
        SAGEFLOW_LOG_WARN("LSHMethod", "超平面数量 num_hashes={} 非法，使用默认值 8", config_.num_hashes);
        config_.num_hashes = 8;
    }
    if (config_.dimension <= 0) {
        SAGEFLOW_LOG_WARN("LSHMethod", "维度 dimension={} 非法，使用默认值 128", config_.dimension);
        config_.dimension = 128;
    }
    if (config_.max_probes_per_table <= 0) {
        SAGEFLOW_LOG_WARN("LSHMethod", "max_probes_per_table={} 非法，使用默认值 64", config_.max_probes_per_table);
        config_.max_probes_per_table = 64;
    }
    if (config_.max_hamming_radius < 0) {
        SAGEFLOW_LOG_WARN("LSHMethod", "max_hamming_radius={} 非法，使用默认值 4", config_.max_hamming_radius);
        config_.max_hamming_radius = 4;
    }
    config_.max_hamming_radius = std::min(config_.max_hamming_radius, config_.num_hashes);
}

void LSHMethod::open(const RuntimeContext& context,
                     WindowState* left_state,
                     WindowState* right_state) {
    subtask_index_ = context.getSubtaskIndex();
    left_state_ = left_state;
    right_state_ = right_state;
    window_size_ms_ = config_.window_size_ms;
    left_buckets_.assign(static_cast<size_t>(config_.num_tables), BucketMap{});
    right_buckets_.assign(static_cast<size_t>(config_.num_tables), BucketMap{});
    initHyperplanes();
    SAGEFLOW_LOG_INFO("LSHMethod", "初始化完成：tables={} hashes/table={} dim={} subtask={}/{}",
                      config_.num_tables, config_.num_hashes, config_.dimension,
                      context.getSubtaskIndex(), context.getParallelism());
}

void LSHMethod::onRecordAdded(const VectorRecord& record, int slot) {
    // slot: 0=left, 1=right. 我们的桶对两侧都存储，查询时按 query_slot 选择对面窗口。
    (void)slot;
    auto record_ptr = std::make_shared<VectorRecord>(record);
    const auto& vec = toFloatVector(*record_ptr);
    if (vec.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(buckets_mutex_);
    auto& target_buckets = (slot == 0) ? left_buckets_ : right_buckets_;
    if (target_buckets.size() != static_cast<size_t>(config_.num_tables)) {
        target_buckets.assign(static_cast<size_t>(config_.num_tables), BucketMap{});
    }
    for (size_t t = 0; t < tables_.size(); ++t) {
        const auto key = hashVector(*record_ptr, tables_[t]);
        auto& bucket = target_buckets[t][key];
        // 去重：同 uid 不重复插入
        bool exists = false;
        for (const auto& ptr : bucket) {
            if (ptr && ptr->uid_ == record_ptr->uid_) {
                exists = true;
                break;
            }
        }
        if (!exists) {
            bucket.push_back(record_ptr);
        }
    }
}

std::vector<std::unique_ptr<VectorRecord>> LSHMethod::ExecuteEager(
    const VectorRecord& query_record,
    int query_slot) {
    std::vector<std::unique_ptr<VectorRecord>> results;
    WindowState* target_state = (query_slot == 0) ? right_state_ : left_state_;
    if (!target_state) {
        SAGEFLOW_LOG_WARN("LSHMethod", "执行时目标窗口为空，slot={}", query_slot);
        return results;
    }

    // 预先提取查询向量，避免在循环中重复拷贝
    const auto query_vec = toFloatVector(query_record);
    if (query_vec.empty()) {
        return results;
    }

    // 预先计算查询向量的桶键
    std::vector<uint64_t> query_keys;
    query_keys.reserve(static_cast<size_t>(config_.num_tables));
    for (const auto& planes : tables_) {
        query_keys.push_back(hashVector(query_record, planes));
    }

    const int64_t window_lower_bound = query_record.timestamp_ - window_size_ms_;

    // 从桶中收集候选（联合多个表的命中）
    std::unordered_map<uint64_t, std::shared_ptr<const VectorRecord>> candidate_map;
    size_t buckets_scanned = 0;
    size_t probes_used = 0;
    {
        std::lock_guard<std::mutex> lock(buckets_mutex_);
        auto& source_buckets = (query_slot == 0) ? right_buckets_ : left_buckets_;
        if (source_buckets.size() != tables_.size()) {
            source_buckets.assign(tables_.size(), BucketMap{});
        }
        for (size_t t = 0; t < tables_.size(); ++t) {
            const auto probe_keys = buildProbeKeys(query_keys[t]);
            size_t probes_for_table = 0;
            for (uint64_t key : probe_keys) {
                if (probes_for_table >= static_cast<size_t>(config_.max_probes_per_table)) {
                    break;
                }
                ++probes_for_table;
                ++probes_used;
                ++buckets_scanned;
                auto map_it = source_buckets[t].find(key);
                if (map_it == source_buckets[t].end()) {
                    continue;
                }
                auto& bucket = map_it->second;
                bucket.erase(std::remove_if(bucket.begin(), bucket.end(), [&](const std::shared_ptr<const VectorRecord>& ptr) {
                    return !ptr || ptr->timestamp_ < window_lower_bound;
                }), bucket.end());
                for (const auto& ptr : bucket) {
                    if (!ptr) continue;
                    if (ptr->uid_ == query_record.uid_) continue;
                    candidate_map.emplace(ptr->uid_, ptr);
                }
            }
        }
    }

    for (const auto& kv : candidate_map) {
        const auto& candidate_ptr = kv.second;
        const auto cand_vec = toFloatVector(*candidate_ptr);
        if (cand_vec.empty() || cand_vec.size() != query_vec.size()) {
            continue;
        }
        const double sim = l2Similarity(query_vec, cand_vec);
        if (sim >= join_similarity_threshold_) {
            results.emplace_back(std::make_unique<VectorRecord>(*candidate_ptr));
        }
    }

    SAGEFLOW_LOG_DEBUG("LSHMethod", "slot={} uid={} probes={} buckets={} candidates={} kept={}",
                      query_slot,
                      query_record.uid_,
                      probes_used,
                      buckets_scanned,
                      candidate_map.size(),
                      results.size());
    return results;
}

void LSHMethod::initHyperplanes() {
    tables_.clear();
    tables_.resize(static_cast<size_t>(config_.num_tables));
    std::mt19937 rng(config_.seed);
    std::normal_distribution<float> dist(0.0f, 1.0f);

    for (auto& table : tables_) {
        table.reserve(static_cast<size_t>(config_.num_hashes));
        for (int h = 0; h < config_.num_hashes; ++h) {
            Hyperplane hp(static_cast<size_t>(config_.dimension));
            for (int d = 0; d < config_.dimension; ++d) {
                hp[static_cast<size_t>(d)] = dist(rng);
            }
            table.push_back(std::move(hp));
        }
    }
}

uint64_t LSHMethod::hashVector(const VectorRecord& record,
                               const std::vector<Hyperplane>& planes) const {
    const auto vec = toFloatVector(record);
    if (vec.size() < planes.size()) {
        return 0;
    }
    uint64_t bits = 0;
    for (size_t i = 0; i < planes.size(); ++i) {
        float dp = dotProduct(vec, planes[i]);
        if (dp >= 0.0f) {
            bits |= (uint64_t(1) << i);
        }
    }
    return bits;
}

std::vector<uint64_t> LSHMethod::buildProbeKeys(uint64_t base_key) const {
    const size_t limit = static_cast<size_t>(std::max(1, config_.max_probes_per_table));
    std::vector<uint64_t> keys;
    keys.reserve(limit);
    keys.push_back(base_key);

    const int bits = std::min(config_.num_hashes, 63);

    auto push_if_room = [&](uint64_t key) {
        if (keys.size() < limit) {
            keys.push_back(key);
        }
    };

    // 多半径探测，按汉明距离从小到大生成组合，直到达到半径或探测上限
    const int max_radius = std::min(config_.max_hamming_radius, bits);
    std::vector<int> indices;
    for (int r = 1; r <= max_radius && keys.size() < limit; ++r) {
        indices.resize(static_cast<size_t>(r));
        std::iota(indices.begin(), indices.end(), 0);

        while (true) {
            uint64_t mask = 0;
            for (int idx : indices) {
                mask |= (uint64_t(1) << idx);
            }
            push_if_room(base_key ^ mask);
            if (keys.size() >= limit) {
                break;
            }

            int i = r - 1;
            while (i >= 0 && indices[static_cast<size_t>(i)] == bits - r + i) {
                --i;
            }
            if (i < 0) {
                break;
            }
            ++indices[static_cast<size_t>(i)];
            for (int j = i + 1; j < r; ++j) {
                indices[static_cast<size_t>(j)] = indices[static_cast<size_t>(j - 1)] + 1;
            }
        }
    }

    return keys;
}

std::vector<float> LSHMethod::toFloatVector(const VectorRecord& record) {
    const auto& vector_data = record.data_;
    const int32_t dim = vector_data.dim_;
    if (dim <= 0) {
        return {};
    }
    const float* float_ptr = reinterpret_cast<const float*>(vector_data.data_.get());
    std::vector<float> result(static_cast<size_t>(dim));
    std::memcpy(result.data(), float_ptr, static_cast<size_t>(dim) * sizeof(float));
    return result;
}

}  // namespace sageFlow

// ==================== 方法自注册 ====================
REGISTER_JOIN_METHOD(
    sageFlow::JoinAlgorithm::LSH,
    (sageFlow::JoinMethodRegistry::MethodInfo{
        "LSH",
        "Hyperplane-based Locality-Sensitive Hashing join (vector cosine). "
        "Uses multiple random hyperplane tables as coarse buckets, then cosine verify.",
        sageFlow::JoinAlgorithm::LSH,
        true,   // supports_eager
        false,  // supports_lazy
        sageFlow::PartitionStrategy::ROUND_ROBIN,
        sageFlow::WindowStateType::SHARED,
        "Charikar 2002 hyperplane LSH"
    }),
    [](const sageFlow::JoinStrategyConfig& config,
       std::shared_ptr<sageFlow::ConcurrencyManager> cm,
       int /*dim*/,
       int /*left_idx*/,
       int /*right_idx*/) {
        (void)cm; // 当前实现不依赖共享索引
        sageFlow::LSHMethod::Config cfg;
        cfg.similarity_threshold = config.similarity_threshold;
        cfg.num_tables = config.lsh_num_tables;
        cfg.num_hashes = config.lsh_num_hashes;
        cfg.dimension = config.dimension;
        cfg.seed = config.lsh_seed;
        return std::make_unique<sageFlow::LSHMethod>(cfg);
    });
