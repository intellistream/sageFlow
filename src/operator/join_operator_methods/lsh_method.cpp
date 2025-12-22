#include "operator/join_operator_methods/lsh_method.h"

#include <cmath>
#include <algorithm>
#include <cstring>

#include "operator/join_method_registry.h"
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
}

void LSHMethod::open(const RuntimeContext& context,
                     WindowState* left_state,
                     WindowState* right_state) {
    subtask_index_ = context.getSubtaskIndex();
    left_state_ = left_state;
    right_state_ = right_state;
    initHyperplanes();
    SAGEFLOW_LOG_INFO("LSHMethod", "初始化完成：tables={} hashes/table={} dim={} subtask={}/{}",
                      config_.num_tables, config_.num_hashes, config_.dimension,
                      context.getSubtaskIndex(), context.getParallelism());
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

    // 预先计算查询向量的桶键
    std::vector<uint64_t> query_keys;
    query_keys.reserve(static_cast<size_t>(config_.num_tables));
    for (const auto& planes : tables_) {
        query_keys.push_back(hashVector(query_record, planes));
    }

    // 获取目标窗口快照，避免长时间持锁（WindowState 内部已做同步）
    auto records_snapshot = target_state->getRecordsSnapshot(subtask_index_);

    for (const auto& candidate_ptr : records_snapshot) {
        if (!candidate_ptr || candidate_ptr->uid_ == query_record.uid_) {
            continue;
        }
        // 简单桶过滤：任意一张表命中即可进入精排
        bool bucket_hit = false;
        for (size_t t = 0; t < tables_.size(); ++t) {
            auto key = hashVector(*candidate_ptr, tables_[t]);
            if (key == query_keys[t]) {
                bucket_hit = true;
                break;
            }
        }
        if (!bucket_hit) continue;

        // 计算余弦相似度
        const auto cand_vec = toFloatVector(*candidate_ptr);
        const auto query_vec = toFloatVector(query_record);
        if (cand_vec.empty() || query_vec.empty()) {
            continue;
        }
        // 使用 SIMD 余弦相似度，加速候选验证
        float sim = SIMDDistance::cosineSimilarity(query_vec.data(), cand_vec.data(), cand_vec.size());
        if (sim >= static_cast<float>(join_similarity_threshold_)) {
            results.emplace_back(std::make_unique<VectorRecord>(*candidate_ptr));
        }
    }

    SAGEFLOW_LOG_DEBUG("LSHMethod", "slot={} 输入 uid={}，候选数={} 通过过滤后={} ",
                      query_slot, query_record.uid_, records_snapshot.size(), results.size());
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
