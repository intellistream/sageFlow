#include <common/data_types.h>
#include <compute_engine/compute_engine.h>
#include <stream/stream_environment.h>
#include <utils/conf_map.h>
#include <utils/monitoring.h>  // Keep for potential detailed monitoring

#ifdef CANDY_ENABLE_METRICS
#include "operator/join_metrics.h"
#endif

#include <algorithm>  // For std::min
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <numeric>  // For std::accumulate  // For data generation
#include <stdexcept>
#include <string>
#include <vector>

#include "function/sink_function.h"
#include "stream/data_stream_source/sift_stream_source.h"  // Include SiftStreamSource explicitly

using namespace std;          // NOLINT
using namespace candy;        // NOLINT
using namespace std::chrono;  // NOLINT

const std::string CANDY_PATH = PROJECT_DIR;
#define CONFIG_DIR "/config/"

namespace candy {

// -------------------------------

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

  const bool genanswer_flag = conf.getString("genanswer") == "true";
  string index_type_str;  // 更改测试的 Index 算法
  index_type_str = conf.getString("indexType");
  string input_path = conf.getString("inputPath");
  const string source_type = "Simple";

  const int dimension = 128;               // SIFT 数据集维度
  const int num_base_vectors = 10000;      // SIFT 数据集向量个数
  const int64_t num_stream_records = 100;  // 查询向量个数， 也就是流里的向量个数
  int k = 100;                             // KNN 中的 K 值

  // ------------------------------------------------------------------

  cout << "--- Streaming Performance Test ---" << endl;
  cout << "Config File: " << config_file_path << endl;
  cout << "Index Type: " << index_type_str << endl;
  cout << "Dimension: " << dimension << endl;
  cout << "Base Vectors (Index Size): " << num_base_vectors << endl;
  cout << "Stream Records to Process: " << num_stream_records << endl;
  cout << "Top K: " << k << endl;
  cout << "Source Type: " << source_type << endl;

  // --------------------------------------------------------------------------------------------

  // ---------------------------------------- 1. Setup Index -------------------------------------------------
  cout << "\nSetting up index..." << endl;
  auto concurrency_manager = env.getConcurrencyManager();  // Get CM from environment

  IndexType index_type_enum;
  if (index_type_str == "HNSW") {
    index_type_enum = IndexType::HNSW;
  } else if (index_type_str == "IVF") {
    index_type_enum = IndexType::IVF;
  } else if (index_type_str == "Vectraflow") {
    index_type_enum = IndexType::Vectraflow;
  } else if (index_type_str == "KNN" || index_type_str == "BruteForce") {
    index_type_enum = IndexType::BruteForce;
  } else {
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

  // ------------------------ Insert base vectors into the index ---------------------------------

  auto build_start = high_resolution_clock::now();
  auto base_vector_source = make_shared<SiftStreamSource>("base_input_source", "./data/siftsmall/siftsmall_base.fvecs");
  base_vector_source->Init();

  cerr << "Base vector source initialized." << endl;

  for (int i = 0; i < num_base_vectors; ++i) {
    auto record = base_vector_source->Next();

    if (!concurrency_manager->insert(index_id, std::move(record))) {
      cerr << "Warning: Failed to insert base vector with UID " << record->uid_ << endl;
      continue;  // Skip this iteration if record is null
    }
  }

  auto build_end = high_resolution_clock::now();
  auto build_duration = duration_cast<milliseconds>(build_end - build_start);
  cout << "Index population finished in " << build_duration.count() / 1000.0 << " s." << endl;

  // ----------------------------------------------------------------------------------------

  // -------------------------------- 2. Setup Stream -------------------------------------------------------------
  cout << "\nSetting up stream pipeline..." << endl;
  shared_ptr<DataStreamSource> source_stream = make_shared<SiftStreamSource>("FilePerfSource", input_path);

  std::atomic<int64_t> processed_count(0);
  int64_t expected_count =
      (source_type == "Simple") ? num_stream_records * k : -1;  // -1 if count is unknown for FileSource
  std::vector<double> latencies_us;                             // Store latencies in microseconds

  auto last_start_time = high_resolution_clock::now();
  bool last_flag = false;
  bool is_testing_cqr = true;
  std::vector<uint64_t> records_ids;

  std ::ofstream genanswer;
  if (genanswer_flag) {
    genanswer.open("./data/siftsmall/answer.txt");
  }

  source_stream->topk(index_id, k)
      ->writeSink(std::make_unique<SinkFunction>(
          "PerfSink", [&processed_count, &last_flag, &last_start_time, &latencies_us, &records_ids, is_testing_cqr,
                       &genanswer, &genanswer_flag](const std::unique_ptr<VectorRecord> &record) {
            if (!record) {
              // if (IsTestingCQR)
              // records_ids.emplace_back(-1);
              return;
            }  // Skip if  record is null
            if (is_testing_cqr) {
              records_ids.emplace_back(record->uid_);
            }
            if (genanswer_flag) {
              genanswer << record->uid_ << endl;
            }

            ++processed_count;
            auto end_time = high_resolution_clock::now();
            if (!last_flag) {
              last_start_time = end_time;
              last_flag = true;
            } else {
              auto latency = duration_cast<microseconds>(end_time - last_start_time).count();
              latencies_us.emplace_back(latency);
              // cerr << latency << " us" << endl;
              last_start_time = end_time;
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

  // ------------------------------- 4. Report Results --------------------------------------------
  // Throughput
  auto execution_duration = duration_cast<milliseconds>(execution_end - execution_start);
  double execution_secs = execution_duration.count() / 1000.0;
  int64_t final_processed_count = processed_count.load();
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

  // Latency
  cout << "\n--- Latency Results ---" << endl;
  assert(!latencies_us.empty() && "Latency data should not be empty");
  if (!latencies_us.empty()) {
    double total_latency_us = std::accumulate(latencies_us.begin(), latencies_us.end(), 0.0);
    double avg_latency_ms = (total_latency_us / latencies_us.size()) / 1000.0;

    // Calculate Percentile Latency (e.g., P95, P99)
    std::sort(latencies_us.begin(), latencies_us.end());
    // for (auto &latency : latencies_us) {
    //   cerr << latency << " us" << endl;
    // }

    int p95pos = static_cast<int>(latencies_us.size() * 0.95);
    int p99pos = static_cast<int>(latencies_us.size() * 0.99);

    if (p95pos >= static_cast<int>(latencies_us.size())) {
      p95pos = latencies_us.size() - 1;
    }
    if (p99pos >= static_cast<int>(latencies_us.size())) {
      p99pos = latencies_us.size() - 1;
    }

    // cerr << p95pos << ' ' << latencies_us[p95pos] / 1000.0 << endl;
    double p95_latency_ms = latencies_us[p95pos] / 1000.0;
    double p99_latency_ms = latencies_us[p99pos] / 1000.0;
    double min_latency_ms = latencies_us.front() / 1000.0;
    double max_latency_ms = latencies_us.back() / 1000.0;

    cout << "Number of Latency Samples: " << latencies_us.size() << endl;
    cout << "Average Latency: " << avg_latency_ms << " ms" << endl;
    cout << "Min Latency: " << min_latency_ms << " ms" << endl;
    cout << "Max Latency: " << max_latency_ms << " ms" << endl;
    cout << "P95 Latency: " << p95_latency_ms << " ms" << endl;
    cout << "P99 Latency: " << p99_latency_ms << " ms" << endl;
  }

  // CQR
  double cqr = 0;
  // TODO(pygone):
  // answer.txt 是由 KNN 生成的 topk 答案文件
  // 启动前先把 genanwer_flag 设置为 true 跑一下生成这个文件再运行

  ifstream groundtruth_file("./data/siftsmall/answer.txt");
  if (!groundtruth_file.is_open()) {
    cerr << "Error opening ground truth file." << endl;
    return;
  }

  cerr << records_ids.size() << endl;

  assert((int)records_ids.size() == num_stream_records * k);
  for (int i = 0; i < num_stream_records; ++i) {
    std::vector<uint64_t> groundtruth;
    std::vector<uint64_t> result;
    for (int j = 0; j < k; ++j) {
      uint64_t id;
      if (groundtruth_file >> id) {
        groundtruth.push_back(id);
      } else {
        cerr << "Error reading ground truth file." << endl;
        return;
      }
    }
    result.reserve(k);
    for (int j = 0; j < k; ++j) {
      result.push_back(records_ids[i * k + j]);
    }
    std::sort(groundtruth.begin(), groundtruth.end());
    std::sort(result.begin(), result.end());
    int cnt = 0;
    for (auto x : result) {
      auto it = std::lower_bound(groundtruth.begin(), groundtruth.end(), x);
      if (it != groundtruth.end() && *it == x) {
        ++cnt;
      }
    }
    cqr += static_cast<double>(cnt) / k;
  }
  cqr /= num_stream_records;
  cout << "CQR: " << cqr << endl;

  // QPS
  double qps = 0;
  if (execution_secs > 0 && num_stream_records > 0) {
    qps = static_cast<double>(num_stream_records) / execution_secs;
  }
  cout << "Queries Per Second (QPS): " << qps << " queries/second" << endl;
}

}  // namespace candy

// Main function remains the same
auto main(int argc, char *argv[]) -> int {
  const std::string default_config_file = CANDY_PATH + CONFIG_DIR + "index_test_config.toml";

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