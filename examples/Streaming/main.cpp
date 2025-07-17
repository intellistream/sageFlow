#include <common/data_types.h>
#include <compute_engine/compute_engine.h>
#include <stream/stream_environment.h>
#include <stream/data_stream_source/file_stream_source.h>
#include <utils/conf_map.h>

#include <iostream>
#include <string>
#include <map>
#include <memory>

#include "function/filter_function.h"
#include "function/map_function.h"
#include "function/sink_function.h"
#include "function/join_function.h"
#include "function/window_function.h"
#include "function/aggregate_function.h"

using namespace std;    // NOLINT

// const std::string CANDY_PATH = PROJECT_DIR;
// #define CONFIG_DIR "/config/"

namespace candy {
void SetupAndRunPipeline(const std::string &config_file_path) {
  // Create simple configuration - we no longer use TOML files
  std::map<std::string, std::string> config;
  config["inputPath"] = "/home/xinyan/candyFlow/data/siftsmall/siftsmall_base.fvecs";
  config["outputPath"] = "/tmp/output";
  config["topK"] = "10";
  config["similarityThreshold"] = "0.8";

  StreamEnvironment env;
  ConfigMap conf(config);
  env.setConfiguration(conf);

  try {
    // Create primary file stream
    auto file_stream = std::make_shared<FileStreamSource>("FileStream", config["inputPath"]);
    
    // Create secondary stream for join operation
    auto join_stream = std::make_shared<FileStreamSource>("JoinStream", config["inputPath"]);
    
    // Build a comprehensive streaming pipeline with chained operators
    file_stream
        // Filter: Only process vectors where first element > 0.5
        ->filter(std::make_unique<FilterFunction>("filter1",
                                                  [](const std::unique_ptr<VectorRecord> &record) -> bool {
                                                    const auto rec = record.get();
                                                    auto* data = reinterpret_cast<float*>(rec->data_.data_.get());
                                                    return data[0] > 0.5F;
                                                  }))
        
        // Map: Transform the vector data (e.g., normalize first element)
        ->map(std::make_unique<MapFunction>(
            "map1", [](std::unique_ptr<VectorRecord> &record) -> std::unique_ptr<VectorRecord> {
              auto* data = reinterpret_cast<float*>(record->data_.data_.get());
              data[0] = data[0] / 10.0F; // Normalize first element
              return std::move(record);
            }))
        
        // Window: Create tumbling windows of 50 records with slide of 10
        ->window(std::make_unique<WindowFunction>("window1", 50, 10, WindowType::Tumbling))
        
        // Aggregate: Calculate average of vectors in each window
        ->aggregate(std::make_unique<AggregateFunction>("aggregate1", AggregateType::Avg))
        
        // Join: Join with secondary stream based on UID
        ->join(join_stream, std::make_unique<JoinFunction>(
                                "join1", [](std::unique_ptr<VectorRecord> &l,
                                            std::unique_ptr<VectorRecord> &r) -> std::unique_ptr<VectorRecord> {
                                              // Join condition: same UID
                                              if (l->uid_ == r->uid_) {
                                                // Return combined record (keeping left record structure)
                                                return std::make_unique<VectorRecord>(l->uid_, l->timestamp_, l->data_);
                                              }
                                              return nullptr;
                                            }))
        
        // TopK: Find top 5 similar vectors using index
        ->topk(0, 5) // index_id=0, k=5
        
        // Sink 1: Output vector UIDs
        ->writeSink(std::make_unique<SinkFunction>(
            "sink1",
            [](const std::unique_ptr<VectorRecord> &record) { 
              std::cout << "Processed Vector UID: " << record->uid_ << std::endl; 
            }))
        
        // Sink 2: Output first element of vector data
        ->writeSink(std::make_unique<SinkFunction>("sink2", 
            [](const std::unique_ptr<VectorRecord> &record) {
              auto* data = reinterpret_cast<float*>(record->data_.data_.get());
              std::cout << "First element: " << data[0] << std::endl;
            }));

    // Add streams to the environment and execute
    env.addStream(file_stream);
    env.addStream(join_stream);
    env.execute();
    
    std::cout << "Pipeline execution completed successfully!" << std::endl;
    
  } catch (const std::exception &e) {
    std::cerr << "Pipeline execution failed: " << e.what() << std::endl;
    throw;
  }
} 
}  // namespace candy
auto main(int argc, char *argv[]) -> int {
  // For now, we don't use command line arguments for config file path
  // We use hardcoded configuration instead
  
  try {
    candy::SetupAndRunPipeline("unused");
  } catch (const exception &e) {
    cerr << "Error: " << e.what() << '\n';
    return 1;
  }

  return 0;
}
