#ifndef STREAM_ENVIRONMENT_HPP
#define STREAM_ENVIRONMENT_HPP

#include <utils/conf_map.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <spdlog/spdlog.h>

#include "common/data_types.h"
#include "concurrency/concurrency_manager.h"
#include "operator/operator.h"
#include "query/optimizer/planner.h"
#include "storage/storage_manager.h"
#include "stream/time/time_api.h"
#include "stream/elements/watermark.h"

namespace candy {

// Forward declarations
class Stream;
class Operator;
class Watermark;
class EndOfStream;

/**
 * @brief The StreamEnvironment class manages stream processing execution
 * 
 * It coordinates stream processing, manages time semantics (event time, watermarks),
 * handles window progression, out-of-order data, monitoring, and multi-input alignment.
 */
class StreamEnvironment {
 public:
  // Constructor to initialize the environment
  explicit StreamEnvironment() {
    storage_manager_ = std::make_unique<StorageManager>();
    concurrency_manager_ = std::make_unique<ConcurrencyManager>(*storage_manager_);
    planner_ = std::make_unique<Planner>(*concurrency_manager_);
    
    // Initialize metrics
    metrics_["throughput"] = 0.0;
    metrics_["latency"] = 0.0;
    metrics_["watermark_lag"] = 0.0;
    metrics_["record_count"] = 0.0;
    
    spdlog::info("StreamEnvironment initialized");
  }

  // Load configuration from a file
  static auto loadConfiguration(const std::string &file_path) -> ConfigMap;

  // Execute the stream processing
  auto execute() -> void;

  // Add a stream to the environment
  auto addStream(std::shared_ptr<Stream> stream) -> void;
  
  // Register an operator to be managed
  auto registerOperator(std::shared_ptr<Operator> op) -> void;
  
  // Start the stream processing
  auto start() -> void;
  
  // Stop the stream processing
  auto stop() -> void;
  
  // Process a watermark through the operator graph
  auto processWatermark(const Watermark& watermark, std::shared_ptr<Operator> source_op) -> void;
  
  // Process an EndOfStream signal through the operator graph
  auto processEndOfStream(std::shared_ptr<Operator> source_op) -> void;
  
  // Get the current global watermark
  auto getCurrentWatermark() const -> timestamp_t;

  // Get the storage manager
  auto getStorageManager() -> StorageManager& { return *storage_manager_; }

  // Get the concurrency manager
  auto getConcurrencyManager() -> ConcurrencyManager& { return *concurrency_manager_; }

  // Get the query planner
  auto getPlanner() -> Planner& { return *planner_; }

  // Set the allowed lateness for late records
  auto setAllowedLateness(timestamp_t lateness) -> void { allowed_lateness_ = lateness; }
  
  // Get the allowed lateness setting
  auto getAllowedLateness() const -> timestamp_t { return allowed_lateness_; }

  // Get metrics from the stream processing
  auto getMetrics() -> std::unordered_map<std::string, double> {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    return metrics_;
  }

 private:
  std::vector<std::shared_ptr<Stream>> streams_;
  std::vector<std::shared_ptr<Operator>> operators_;

  // Track watermarks per operator
  std::unordered_map<std::shared_ptr<Operator>, timestamp_t> operator_watermarks_;

  // Global watermark is the minimum across all sources
  std::atomic<timestamp_t> global_watermark_{0};

  // Allowed lateness configuration
  timestamp_t allowed_lateness_{0};

  // Running state
  std::atomic<bool> running_{false};

  // Manager components
  std::unique_ptr<StorageManager> storage_manager_;
  std::unique_ptr<Planner> planner_;
  std::unique_ptr<ConcurrencyManager> concurrency_manager_;

  // Metrics tracking
  std::unordered_map<std::string, double> metrics_;
  mutable std::mutex metrics_mutex_;

  // Update metrics
  void updateMetric(const std::string& name, double value);

  // Build the operator graph from streams
  void buildOperatorGraph();

  // Create operators for a stream and its children
  void createOperators(std::shared_ptr<Stream> stream);
  
  // Update the global watermark based on individual operator watermarks
  void updateGlobalWatermark();
};

}  // namespace candy

#endif  // STREAM_ENVIRONMENT_HPP