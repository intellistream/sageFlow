#include <common/data_types.h>
#include <compute_engine/compute_engine.h>
#include <stream/stream_environment.h>
#include <utils/conf_map.h>
#include <utils/monitoring.h>  // Keep for potential detailed monitoring

#include <algorithm>  // For std::min
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>    // For thread safety
#include <numeric>  // For std::accumulate  // For data generation
#include <random>   // For data generation
#include <stdexcept>
#include <string>
#include <unordered_map>  // For start time tracking
#include <vector>

#include "concurrency/concurrency_manager.h"  // Include ConcurrencyManager explicitly
#include "function/map_function.h"            // Include MapFunction
#include "function/sink_function.h"
#include "function/window_function.h"
#include "stream/data_stream_source/file_stream_source.h"
#include "stream/data_stream_source/sift_stream_source.h"  // Include SiftStreamSource explicitly
#include "stream/data_stream_source/simple_stream_source.h"

using namespace std;    // NOLINT
using namespace candy;  // NOLINT
using namespace std::chrono;

const std::string CANDY_PATH = PROJECT_DIR;
#define CONFIG_DIR "/config/"

namespace candy {

const string KEY_INDEX_TYPE = "indexType";  // e.g., "HNSW", "IVF", "KNN", "Vectraflow"
const string KEY_DIMENSION = "dimension";
const string KEY_NUM_BASE_VECTORS = "numBaseVectors";      // Vectors to build the index
const string KEY_NUM_STREAM_RECORDS = "numStreamRecords";  // Records to process in the stream
const string KEY_TOP_K = "topK";
const string KEY_SOURCE_TYPE = "sourceType";  // "Simple" or "File"
const string KEY_INPUT_PATH = "inputPath";    // Required if sourceType is "File"

void ValidateConfiguration(const ConfigMap &conf) {
  cerr << "Validating configuration..." << endl;
  if (!conf.exist("inputPath") || !conf.exist("outputPath")) {
    throw runtime_error("Missing required configuration keys: inputPath or outputPath.");
  }
  if (!conf.exist("topK")) {
    throw runtime_error("Missing required configuration key: topK.");
  }
  if (!conf.exist("similarityThreshold")) {
    throw runtime_error("Missing required configuration key: similarityThreshold.");
  }
}

void SetupAndRunPipeline(const std::string &config_file_path) {
  StreamEnvironment env;

  const auto conf = candy::StreamEnvironment::loadConfiguration(config_file_path);
  try {
    cerr << "Loading configuration..." << endl;
    ValidateConfiguration(conf);  // Use the updated validation function
  } catch (const exception &e) {
    cerr << "Configuration Error: " << e.what() << endl;
    throw;
  }

  // --------------------------- 配置 ---------------------------------

  string index_type_str = "HNSW";      // 更改测试的 Index 算法
  const int dimension = 128;           // SIFT 数据集维度
  const int num_base_vectors = 10000;  // SIFT 数据集向量个数
  long num_stream_records = 100;       // 查询向量个数， 也就是流里的向量个数
  int k = 10;                          // TopK
  // ------------------------------------------------------------------

  string source_type = "Simple";

  cout << "--- Streaming Performance Test ---" << endl;
  cout << "Config File: " << config_file_path << endl;
  cout << "Index Type: " << index_type_str << endl;
  cout << "Dimension: " << dimension << endl;
  cout << "Base Vectors (Index Size): " << num_base_vectors << endl;
  cout << "Stream Records to Process: " << num_stream_records << endl;
  cout << "Top K: " << k << endl;
  cout << "Source Type: " << source_type << endl;
  if (source_type == "File") {
    cout << "Input Path: " << conf.getString(KEY_INPUT_PATH) << endl;
  }
  // ------------------------------------ Print index-specific params if loaded
  // ------------------------------------------------------------------------------------

  // ---------------------------------------- 1. Setup Index -------------------------------------------------
  cout << "\nSetting up index..." << endl;
  auto concurrency_manager = env.getConcurrencyManager();  // Get CM from environment

  IndexType index_type_enum;
  if (index_type_str == "HNSW")
    index_type_enum = IndexType::HNSW;
  else if (index_type_str == "IVF")
    index_type_enum = IndexType::IVF;
  else if (index_type_str == "Vectraflow")
    index_type_enum = IndexType::Vectraflow;
  else if (index_type_str == "KNN" || index_type_str == "BruteForce")
    index_type_enum = IndexType::BruteForce;
  else {
    throw runtime_error("Unsupported index type: " + index_type_str);
  }

  string index_name = "streaming_perf_index";
  int index_id = concurrency_manager->create_index(index_name, index_type_enum, dimension);
  if (index_id < 0) {
    throw runtime_error("Failed to create index '" + index_name + "'");
  }
  cout << "Index '" << index_name << "' created with ID: " << index_id << endl;

  // Insert base vectors into the index
  cout << "Populating index with " << num_base_vectors << " base vectors..." << endl;
  auto build_start = high_resolution_clock::now();

  string input_path = conf.getString(KEY_INPUT_PATH);

  auto base_vector_source = make_shared<SiftStreamSource>("base_input_source", "./data/siftsmall/siftsmall_base.fvecs");
  base_vector_source->Init();

  cerr << "Base vector source initialized." << endl;

  // ------------------------ Insert base vectors into the index ---------------------------------

  for (int i = 0; i < num_base_vectors; ++i) {
    uint64_t uid = i + 1;  // Simple UIDs
    // auto record = generate_dummy_vector(uid, dimnsion);
    auto record = base_vector_source->Next();
    if (!record) {
      cerr << "Error: Failed to generate or read base vector with UID " << uid << endl;
      continue;  // Skip this iteration if record is null
    }

    if (!concurrency_manager->insert(index_id, record)) {
      // Handle potential insertion failure
      cerr << "Warning: Failed to insert base vector with UID " << uid << endl;
    }
    // 'record' is likely moved (nulled) after successful insert
  }

  // ----------------------------------------------------------

  auto build_end = high_resolution_clock::now();
  auto build_duration = duration_cast<milliseconds>(build_end - build_start);
  cout << "Index population finished in " << build_duration.count() / 1000.0 << " s." << endl;

  // -------------------------------- 2. Setup Stream
  // --------------------------------------------------------------------------------------------------------------
  cout << "\nSetting up stream pipeline..." << endl;
  shared_ptr<DataStreamSource> source_stream;

  assert(source_type == "Simple");

  if (source_type == "Simple") {
    if (!conf.exist(KEY_INPUT_PATH)) {
      throw runtime_error(
          "SimpleStreamSource workaround requires inputPath in config pointing to pre-generated data file.");
    }

    // TODO: 这里改了 input_path

    input_path = "./data/siftsmall/siftsmall_query.fvecs";
    cout << "Using SimpleStream (as workaround for Simple) reading from: " << input_path << endl;
    cout << "Ensure the file contains approx. " << num_stream_records << " records." << endl;

    source_stream = make_shared<SiftStreamSource>("FilePerfSource", input_path);

  } else {
    throw runtime_error("Unsupported source type: " + source_type);
  }

  // Atomic counter for processed records in the sink
  std::atomic<long> processed_count(0);
  long expected_count =
      (source_type == "Simple") ? num_stream_records * k : -1;  // -1 if count is unknown for FileSource

  // Define the pipeline: source -> topk -> sink

  // ------------------------ 设置运行环境 -----------------------------------------

  // --- Shared state for latency measurement ---
  std::unordered_map<uint64_t, high_resolution_clock::time_point> start_times;
  std::vector<double> latencies_us;  // Store latencies in microseconds
  std::mutex data_mutex;             // Mutex to protect start_times and latencies_us
  // -------------------------------------------

  // Atomic counter for processed records in the sink
  // std::atomic<long> processed_count(0);
  // Expected count should be num_stream_records (queries) * k (results per query) if topk outputs k results
  // Or just num_stream_records if topk outputs one result structure per query.
  // Let's assume the sink counts *results* for now. Adjust if needed.
  // long expected_count = num_stream_records * k;
  // long expected_count = -1; // Set to -1 as we don't know exact output count easily

  // Define the pipeline: source -> map (record start time) -> topk -> sink (calculate latency)
  cout << "Pipeline configured: Source -> Map(RecordStartTime) -> topk(k=" << k << ", index=" << index_id
       << ") -> Sink(CalcLatency)" << endl;

  source_stream
      ->map(std::make_unique<MapFunction>("RecordStartTime",
                                          [&start_times, &data_mutex](std::unique_ptr<VectorRecord> &record) {
                                            if (record) {
                                              auto now = high_resolution_clock::now();
                                              std::lock_guard<std::mutex> lock(data_mutex);
                                              start_times[record->uid_] = now;
                                            }
                                            // Pass the record through
                                          }))
      ->topk(index_id, k)
      ->writeSink(std::make_unique<SinkFunction>(
          "PerfSink",
          [&processed_count, &start_times, &latencies_us, &data_mutex](const std::unique_ptr<VectorRecord> &record) {
            if (!record) return;  // Skip if record is null

            auto end_time = high_resolution_clock::now();
            processed_count.fetch_add(1, std::memory_order_relaxed);
            std::lock_guard<std::mutex> lock(data_mutex);
            auto it = start_times.find(record->uid_);
            if (it != start_times.end()) {
              auto start_time = it->second;
              double latency = duration_cast<microseconds>(end_time - start_time).count();
              latencies_us.push_back(latency);
              start_times.erase(it);
            } else {
              cerr << "Warning: Start time not found for result UID: " << record->uid_ << endl;
            }
          }));
  // Add the source stream to the environment
  env.addStream(std::move(source_stream));
  cout << "Pipeline configured: Source -> topk(k=" << k << ", index=" << index_id << ") -> Sink" << endl;

  // --- 3. Execute Stream and Measure ---
  cout << "\nExecuting stream..." << endl;
  PerformanceMonitor monitor;  // Use the monitor provided by the framework
  monitor.StartProfiling();

  auto execution_start = high_resolution_clock::now();

  try {
    env.execute();  // Run the stream processing
  } catch (const std::exception &e) {
    cerr << "Stream execution failed: " << e.what() << endl;
    monitor.StopProfiling();  // Stop monitor on error too
    throw;                    // Re-throw after logging
  }

  auto execution_end = high_resolution_clock::now();
  monitor.StopProfiling();  // Stop monitor after successful execution

  cout << "\nStream execution finished." << endl;

  // --- 4. Report Results ---
  auto execution_duration = duration_cast<milliseconds>(execution_end - execution_start);
  double execution_secs = execution_duration.count() / 1000.0;
  long final_processed_count = processed_count.load();
  double throughput = (execution_secs > 0 && final_processed_count > 0) ? (final_processed_count / execution_secs) : 0;

  cout << "\n--- Results ---" << endl;
  cout << "Total Execution Time: " << execution_secs << " s" << endl;
  cout << "Total Records Processed by Sink: " << final_processed_count << endl;
  if (expected_count > 0 && final_processed_count != expected_count && source_type == "Simple") {
    // Only warn for SimpleSource where count is known
    cout << "Warning: Processed count (" << final_processed_count << ") does not match expected count ("
         << expected_count << ")" << endl;
  }
  cout << "Throughput (topk pipeline): " << throughput << " records/second" << endl;

  // Print results from PerformanceMonitor if it provides relevant info
  cout << "\n--- Performance Monitor Details ---" << endl;
  cout << "\n--- Latency Results ---" << endl;
  if (!latencies_us.empty()) {
    double total_latency_us = std::accumulate(latencies_us.begin(), latencies_us.end(), 0.0);
    double avg_latency_ms = (total_latency_us / latencies_us.size()) / 1000.0;

    // Calculate Percentile Latency (e.g., P95, P99)
    std::sort(latencies_us.begin(), latencies_us.end());
    double p95_latency_ms = latencies_us[static_cast<size_t>(latencies_us.size() * 0.95)] / 1000.0;
    double p99_latency_ms = latencies_us[static_cast<size_t>(latencies_us.size() * 0.99)] / 1000.0;
    double min_latency_ms = latencies_us.front() / 1000.0;
    double max_latency_ms = latencies_us.back() / 1000.0;

    cout << "Number of Latency Samples: " << latencies_us.size() << endl;
    cout << "Average Latency: " << avg_latency_ms << " ms" << endl;
    cout << "Min Latency: " << min_latency_ms << " ms" << endl;
    cout << "Max Latency: " << max_latency_ms << " ms" << endl;
    cout << "P95 Latency: " << p95_latency_ms << " ms" << endl;
    cout << "P99 Latency: " << p99_latency_ms << " ms" << endl;

    // Check if any start times were not found (indicates potential issue)
    std::lock_guard<std::mutex> lock(data_mutex);  // Lock needed to safely check start_times size
    if (!start_times.empty()) {
      cerr << "Warning: " << start_times.size() << " start times remained in the map after execution." << endl;
    }

  } else {
    cout << "No latency data collected." << endl;
    // Check if start_times map has entries, indicating map ran but sink didn't find matches
    std::lock_guard<std::mutex> lock(data_mutex);
    if (!start_times.empty()) {
      cerr << "Warning: " << start_times.size() << " start times were recorded but no matching results arrived at sink."
           << endl;
    }
  }

  // monitor.PrintProfilingResult();
}

}  // namespace candy

// Main function remains the same
auto main(int argc, char *argv[]) -> int {
  const std::string default_config_file = CANDY_PATH + CONFIG_DIR + "default_config.toml";

  string config_file_path;
  if (argc < 2) {
    config_file_path = default_config_file;
  } else {
    config_file_path = CANDY_PATH + CONFIG_DIR + string(argv[1]);
  }

  try {
    SetupAndRunPipeline(config_file_path);
  } catch (const exception &e) {
    cerr << "Error: " << e.what() << '\n';
    return 1;
  }

  return 0;
}
