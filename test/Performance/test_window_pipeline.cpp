#include <common/data_types.h>
#include <compute_engine/compute_engine.h>
#include <stream/stream_environment.h>
#include <utils/conf_map.h>
#include <utils/monitoring.h>
#include <function/window_function.h>
#include <stream/data_stream_source/sift_stream_source.h>
#include <gtest/gtest.h>
#include <memory>
#include <atomic>
#include <chrono>
#include <vector>
#include <mutex>
#include <unordered_map>
#include <iostream>
#include <fstream>

using namespace candy;
using namespace std;
using namespace std::chrono;

TEST(WindowTest, TumblingWindowPipeline) {
    StreamEnvironment env;

    // Create a simple stream source
    auto input_path = "./data/siftsmall/siftsmall_query.fvecs";
    cout << "Using SimpleStream (as workaround for Simple) reading from: " << input_path << endl;

    auto source_stream = make_shared<SiftStreamSource>("FilePerfSource", input_path);
    // Atomic counter for processed records
    atomic<long> processed_count(0);

    // Shared state for latency measurement
    unordered_map<uint64_t, high_resolution_clock::time_point> start_times;
    vector<double> latencies_us;
    mutex data_mutex;

    // Define the pipeline: source -> window -> sink
    source_stream
        ->window(make_unique<WindowFunction>("TumblingWindow", 10, 0, WindowType::Tumbling))
        ->writeSink(make_unique<SinkFunction>("Sink", [&processed_count, &start_times, &latencies_us, &data_mutex](const unique_ptr<VectorRecord>& record) {
            if (!record) return;

            auto end_time = high_resolution_clock::now();
            processed_count.fetch_add(1, memory_order_relaxed);
            lock_guard<mutex> lock(data_mutex);
            std::cout<<"GET:"<<record->uid_<<std::endl;
            auto it = start_times.find(record->uid_);
            if (it != start_times.end()) {
                auto start_time = it->second;
                double latency = duration_cast<microseconds>(end_time - start_time).count();
                latencies_us.push_back(latency);
                start_times.erase(it);
            }
        }));

    env.addStream(move(source_stream));

    // Execute the pipeline
    env.execute();

    // Logging results to file
    std::ofstream log_file("test_window_pipeline.log", std::ios::out);

    if (processed_count.load() > 0) {
        log_file << "Processed count: " << processed_count.load() << std::endl;
    } else {
        log_file << "Error: No records processed." << std::endl;
    }

    if (!latencies_us.empty()) {
        log_file << "Latencies recorded: " << latencies_us.size() << " entries." << std::endl;
    } else {
        log_file << "Error: No latencies recorded." << std::endl;
    }
}

TEST(WindowTest, SlidingWindowPipeline) {
    StreamEnvironment env;

    // Create a simple stream source    
    auto input_path = "./data/siftsmall/siftsmall_query.fvecs";
    cout << "Using SimpleStream (as workaround for Simple) reading from: " << input_path << endl;

    auto source_stream = make_shared<SiftStreamSource>("FilePerfSource", input_path);
    // Atomic counter for processed records
    atomic<long> processed_count(0);

    // Shared state for latency measurement
    unordered_map<uint64_t, high_resolution_clock::time_point> start_times;
    vector<double> latencies_us;
    mutex data_mutex;

    // Define the pipeline: source -> window -> sink
    source_stream
        ->window(make_unique<WindowFunction>("SlidingWindow", 10, 5, WindowType::Sliding))
        ->writeSink(make_unique<SinkFunction>("Sink", [&processed_count, &start_times, &latencies_us, &data_mutex](const unique_ptr<VectorRecord>& record) {
            if (!record) return;

            auto end_time = high_resolution_clock::now();
            processed_count.fetch_add(1, memory_order_relaxed);

            lock_guard<mutex> lock(data_mutex);
            std::cout<<"GET:"<<record->uid_<<std::endl;

            auto it = start_times.find(record->uid_);
            if (it != start_times.end()) {
                auto start_time = it->second;
                double latency = duration_cast<microseconds>(end_time - start_time).count();
                latencies_us.push_back(latency);
                start_times.erase(it);
            }
        }));

    env.addStream(move(source_stream));

    // Execute the pipeline
    env.execute();

    // Logging results to file
    std::ofstream log_file("test_window_pipeline.log", std::ios::out);

    if (processed_count.load() > 0) {
        log_file << "Processed count: " << processed_count.load() << std::endl;
    } else {
        log_file << "Error: No records processed." << std::endl;
    }

    if (!latencies_us.empty()) {
        log_file << "Latencies recorded: " << latencies_us.size() << " entries." << std::endl;
    } else {
        log_file << "Error: No latencies recorded." << std::endl;
    }
}