#include "operator/join_operator_methods/lsh_method.h"

#include <cmath>
#include <algorithm>
#include <cstring>
#include <unordered_set>
#include <unordered_map>
#include <mutex>
#include <numeric>
#include <bitset>
#include <functional>

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
    if (config_.sketch_bits <= 0 || config_.sketch_bits > 63) {
        SAGEFLOW_LOG_WARN("LSHMethod", "sketch_bits={} 非法，禁用 sketch 预过滤", config_.sketch_bits);
        config_.sketch_bits = 0;
    }
    if (config_.max_sketch_hamming >= 0) {
        config_.max_sketch_hamming = std::min(config_.max_sketch_hamming, config_.sketch_bits);
        use_sketch_filter_ = config_.sketch_bits > 0;
    } else {
        use_sketch_filter_ = false;  // 显式禁用，避免 recall 损失
    }

    // 设定左右签名位数（偶/奇位分片，贴近 Danny 的张量拆分，用于跨表去重）
    left_bits_ = (config_.num_hashes + 1) / 2;   // 偶数位数量
    right_bits_ = config_.num_hashes / 2;        // 奇数位数量
    left_mask_ = (left_bits_ >= 32) ? 0xFFFFFFFFu : ((1u << left_bits_) - 1u);
    right_mask_ = (right_bits_ >= 32) ? 0xFFFFFFFFu : ((1u << right_bits_) - 1u);
}

void LSHMethod::open(const RuntimeContext& context,
                     WindowState* left_state,
                     WindowState* right_state) {
    subtask_index_ = context.getSubtaskIndex();
    left_state_ = left_state;
    right_state_ = right_state;
    window_size_ms_ = config_.window_size_ms;
    left_buckets_.clear();
    right_buckets_.clear();
    left_bucket_mutexes_.clear();
    right_bucket_mutexes_.clear();
    left_buckets_.resize(static_cast<size_t>(config_.num_tables));
    right_buckets_.resize(static_cast<size_t>(config_.num_tables));
    for (int i = 0; i < config_.num_tables; ++i) {
        left_bucket_mutexes_.emplace_back(std::make_unique<std::mutex>());
        right_bucket_mutexes_.emplace_back(std::make_unique<std::mutex>());
    }
    initHyperplanes();
    if (use_sketch_filter_) {
        initSketchPlanes();
    }
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
    auto& target_buckets = (slot == 0) ? left_buckets_ : right_buckets_;
    auto& bucket_mutexes = (slot == 0) ? left_bucket_mutexes_ : right_bucket_mutexes_;
    if (target_buckets.size() != static_cast<size_t>(config_.num_tables)) {
        SAGEFLOW_LOG_ERROR("LSHMethod", "Buckets not initialized, call open() before onRecordAdded");
        return;
    }
    const uint64_t sketch = use_sketch_filter_ ? sketchVector(*record_ptr) : 0; // 轻量 sketch 预过滤
    for (size_t t = 0; t < tables_.size(); ++t) {
        const auto key = hashVector(*record_ptr, tables_[t]); // 主哈希：超平面符号串
        uint16_t left_sig = 0, right_sig = 0;
        splitHash(key, left_sig, right_sig);
        auto& mutex_ptr = bucket_mutexes[t];
        std::lock_guard<std::mutex> lock(*mutex_ptr);
        auto& bucket = target_buckets[t][key];
        bool exists = false; // 同 UID 只存一份
        for (const auto& entry : bucket) {
            if (entry.record && entry.record->uid_ == record_ptr->uid_) {
                exists = true;
                break;
            }
        }
        if (!exists) {
            bucket.push_back(Entry{record_ptr, key, sketch, left_sig, right_sig});
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
    auto& source_buckets = (query_slot == 0) ? right_buckets_ : left_buckets_;
    auto& bucket_mutexes = (query_slot == 0) ? right_bucket_mutexes_ : left_bucket_mutexes_;
    if (source_buckets.size() != tables_.size()) {
        SAGEFLOW_LOG_ERROR("LSHMethod", "Buckets not initialized, call open() before ExecuteEager");
        return results;
    }
    const uint64_t query_sketch = use_sketch_filter_ ? sketchVector(query_record) : 0; // 查询侧 sketch
    std::unordered_set<uint64_t> seen_tensor;  // Danny 风格重复控制：签名 + UID 去重

    // 自适应扫描：先按配置探测，再根据候选量不足做补偿（放宽半径与 Sketch）
    const size_t kSketchBucketGate = 4;   // 桶很小则跳过 sketch，避免过滤过严
    const size_t kRecallTarget = 8;       // 目标候选量，不足时触发补偿

    auto scan_with_params = [&](int radius,
                                size_t max_probes_per_table,
                                bool allow_sketch,
                                bool is_fallback) {
        for (size_t t = 0; t < tables_.size(); ++t) {
            const auto probe_keys = buildProbeKeys(query_keys[t], radius, max_probes_per_table);
            size_t probes_for_table = 0;
            for (uint64_t key : probe_keys) {
                if (probes_for_table >= max_probes_per_table) {
                    break;
                }
                ++probes_for_table;
                ++probes_used;
                ++buckets_scanned;
                std::lock_guard<std::mutex> lock(*bucket_mutexes[t]);
                auto map_it = source_buckets[t].find(key);
                if (map_it == source_buckets[t].end()) {
                    continue;
                }
                auto& bucket = map_it->second;
                if (!bucket.empty() && bucket.front().record &&
                    bucket.front().record->timestamp_ < window_lower_bound) {
                    bucket.erase(std::remove_if(bucket.begin(), bucket.end(), [&](const Entry& entry) {
                        return !entry.record || entry.record->timestamp_ < window_lower_bound;
                    }), bucket.end()); // 窗口淘汰（仅在看到过期头部时触发，降低开销）
                }
                const bool apply_sketch = use_sketch_filter_ && allow_sketch && bucket.size() >= kSketchBucketGate;
                for (const auto& entry : bucket) {
                    const auto& ptr = entry.record;
                    if (!ptr) continue;
                    if (ptr->uid_ == query_record.uid_) continue;
                    // Danny-like already_seen：左右签名 + UID 组合，避免多表重复验证
                    if (left_bits_ + right_bits_ > 0) {
                        const uint32_t tensor_sig = (static_cast<uint32_t>(entry.left_sig) << 16) | entry.right_sig;
                        const uint64_t seen_key = (static_cast<uint64_t>(tensor_sig) << 32) ^
                                                  static_cast<uint64_t>(std::hash<uint64_t>{}(ptr->uid_));
                        if (!seen_tensor.insert(seen_key).second) {
                            continue;
                        }
                    }
                    if (apply_sketch) {
                        const uint64_t sketch = entry.sketch;
                        const int hd = static_cast<int>(__builtin_popcountll(query_sketch ^ sketch));
                        if (hd > config_.max_sketch_hamming) {
                            continue;
                        }
                    }
                    candidate_map.emplace(ptr->uid_, ptr);
                }
            }
        }
        if (is_fallback) {
            SAGEFLOW_LOG_DEBUG("LSHMethod", "fallback_scan radius={} max_probes={} candidates={} sketch={}",
                               radius, max_probes_per_table, candidate_map.size(), allow_sketch);
        }
    };

    // 主扫描：按配置半径 + Sketch
    scan_with_params(config_.max_hamming_radius,
                     static_cast<size_t>(config_.max_probes_per_table),
                     true,
                     false);

    // 补偿扫描：候选不足时放宽半径并关闭 Sketch 过滤以提升召回
    if (candidate_map.size() < kRecallTarget) {
        const int fallback_radius = std::min(config_.num_hashes, config_.max_hamming_radius + 2);
        const size_t fallback_probes = std::min<size_t>(
            static_cast<size_t>(config_.max_probes_per_table) * 2,
            1024);
        scan_with_params(fallback_radius, fallback_probes, /*allow_sketch=*/false, /*is_fallback=*/true);
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

void LSHMethod::initSketchPlanes() {
    sketch_planes_.clear();
    sketch_planes_.reserve(static_cast<size_t>(config_.sketch_bits));
    std::mt19937 rng(config_.seed + 1);  // 与主超平面区分
    std::normal_distribution<float> dist(0.0f, 1.0f);
    for (int i = 0; i < config_.sketch_bits; ++i) {
        Hyperplane hp(static_cast<size_t>(config_.dimension));
        for (int d = 0; d < config_.dimension; ++d) {
            hp[static_cast<size_t>(d)] = dist(rng);
        }
        sketch_planes_.push_back(std::move(hp));
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

void LSHMethod::splitHash(uint64_t full_hash, uint16_t& left, uint16_t& right) const {
    // 采用偶/奇位交错拆分：偶位→左签名，奇位→右签名，保持 Danny 的“结构分片”思路
    left = 0;
    right = 0;
    for (int bit = 0; bit < config_.num_hashes; ++bit) {
        const bool on = (full_hash >> bit) & 1ULL;
        const int slot = bit / 2;  // 每侧位序
        if ((bit & 1) == 0) {  // 偶位 → 左
            if (slot < 16 && on) {
                left |= static_cast<uint16_t>(1u << slot);
            }
        } else {               // 奇位 → 右
            if (slot < 16 && on) {
                right |= static_cast<uint16_t>(1u << slot);
            }
        }
    }
}

uint64_t LSHMethod::sketchVector(const VectorRecord& record) const {
    const auto vec = toFloatVector(record);
    if (vec.size() < sketch_planes_.size()) {
        return 0;
    }
    uint64_t bits = 0;
    for (size_t i = 0; i < sketch_planes_.size(); ++i) {
        float dp = dotProduct(vec, sketch_planes_[i]);
        if (dp >= 0.0f) {
            bits |= (uint64_t(1) << i);
        }
    }
    return bits;
}

std::vector<uint64_t> LSHMethod::buildProbeKeys(uint64_t base_key,
                                                int max_radius,
                                                size_t max_probes) const {
    const size_t limit = std::max<size_t>(1, max_probes);
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
    const int radius = std::min(max_radius, bits);
    std::vector<int> indices;
    for (int r = 1; r <= radius && keys.size() < limit; ++r) {
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
        "Hyperplane-based Locality-Sensitive Hashing join (cosine). "
        "Uses multiple random hyperplane tables as coarse buckets, then cosine verify.",
        sageFlow::JoinAlgorithm::LSH,
        true,   // supports_eager
        false,  // supports_lazy
        sageFlow::PartitionStrategy::LSH,
        sageFlow::WindowStateType::PARTITIONED_VECTOR,
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
        cfg.window_size_ms = config.window_size_ms;
        return std::make_unique<sageFlow::LSHMethod>(cfg);
    });
