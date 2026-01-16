#pragma once

#include "operator/join_operator_methods/base_method.h"
#include "operator/join_operator_methods/s3j_components/adaptive_partitioner.h"
#include "operator/join_operator_methods/s3j_components/adaptive_index_selector.h"
#include "concurrency/concurrency_manager.h"
#include "state/window_state.h"
#include "execution/runtime_context.h"
#include "index/partitioned_index.h"
#include "state/partitioned_vector_state.h"
#include "state/two_tier_window_state.h"
#include "coordination/workset_directory.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>

namespace sageFlow {

struct S3JConfig {
    double similarity_threshold = 0.8;
    int num_partitions = 16;
    int64_t adapt_interval_ms = 1000;
    double load_threshold = 0.3;
    double index_switch_threshold = 0.2;
    bool enable_metrics = true;
    bool enable_adaptive = true;
    int dimension = 128;
    int nlist = 100;
    int nprobes = 10;
};

struct S3JMetrics {
    double avg_latency_ms = 0.0;
    double throughput_qps = 0.0;
    double recall_estimate = 0.0;
    int current_partitions = 0;
    std::string current_index_type;
    std::vector<AdaptHistory> adapt_history;
    size_t total_queries = 0;
    size_t total_matches = 0;
};

class S3JMethod final : public BaseMethod {
public:
    S3JMethod(int left_index_id,
              int right_index_id,
              double threshold,
              const std::shared_ptr<ConcurrencyManager>& concurrency_manager,
              const S3JConfig& config = S3JConfig());
    
    explicit S3JMethod(double threshold, const S3JConfig& config = S3JConfig());
    
    ~S3JMethod() override = default;
    
    S3JMethod(const S3JMethod&) = delete;
    S3JMethod& operator=(const S3JMethod&) = delete;
    
    std::string getName() const { return "S3J"; }
    
    void open(const RuntimeContext& context,
              WindowState* left_state,
              WindowState* right_state);
    
    std::vector<std::unique_ptr<VectorRecord>> ExecuteEager(
        const VectorRecord& query_record,
        int query_slot) override;
    
    void close();
    S3JMetrics getMetrics() const;
    void forceAdapt();
    void setConcurrencyManager(const std::shared_ptr<ConcurrencyManager>& manager);
    void setWindowStates(WindowState* left_state, WindowState* right_state);
    void setWorksetDirectory(std::shared_ptr<WorksetDirectory> dir);
    const S3JConfig& getConfig() const { return config_; }
    bool isInitialized() const { return initialized_; }

private:
    S3JConfig config_;
    int left_index_id_ = -1;
    int right_index_id_ = -1;
    WindowState* left_state_ = nullptr;
    WindowState* right_state_ = nullptr;
    size_t subtask_index_ = 0;
    size_t parallelism_ = 1;
    bool initialized_ = false;
    
    std::shared_ptr<AdaptivePartitioner> partitioner_;
    std::shared_ptr<AdaptiveIndexSelector> index_selector_;
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;
    std::shared_ptr<WorksetDirectory> workset_directory_;
    IndexType current_index_type_ = IndexType::IVF;
    
    // Per-Workset Load Tracking
    std::unordered_map<uint64_t, std::atomic<size_t>> local_workset_loads_;
    mutable std::mutex stats_mutex_;
    
    struct MetricsCollector {
        std::atomic<size_t> query_count{0};
        std::atomic<int64_t> total_latency_us{0};
        std::atomic<size_t> match_count{0};
        std::chrono::steady_clock::time_point start_time;
        
        void reset() {
            query_count.store(0);
            total_latency_us.store(0);
            match_count.store(0);
            start_time = std::chrono::steady_clock::now();
        }
    };
    mutable MetricsCollector metrics_collector_;
    
    int otherIndexId(int slot) const;
    void maybeAdapt();
    
    std::vector<std::unique_ptr<VectorRecord>> searchInWindowState(
        const VectorRecord& query, int slot);
    
    double computeCosineSimilarity(const std::vector<float>& a, const std::vector<float>& b) const;
    std::vector<float> extractFloatVector(const VectorRecord& record) const;
    
    void scanTierForMatches(const VectorRecord& query, 
                            TwoTierWindowState* tier, 
                            float threshold,
                            std::vector<std::unique_ptr<VectorRecord>>& results);
};
}  // namespace sageFlow
