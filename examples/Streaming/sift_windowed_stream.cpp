#include <common/data_types.h>
#include <compute_engine/compute_engine.h>
#include <stream/stream_environment.h>
#include <utils/conf_map.h>
#include <utils/monitoring.h>

#include <iostream>
#include <stdexcept>
#include <string>
#include <memory>
#include <vector>
#include <chrono>
#include <thread>

#include "function/filter_function.h"
#include "function/map_function.h"
#include "function/sink_function.h"
#include "function/window_function.h"
#include "stream/data_stream_source/sift_stream_source.h"

using namespace std;    // NOLINT
using namespace candy;  // NOLINT

const std::string CANDY_PATH = PROJECT_DIR;
#define CONFIG_DIR "/config/"

namespace candy {

// Global flag to stop processing if needed
std::atomic<bool> should_stop{false};

// Function to verify SIFT data configuration
void ValidateSiftConfiguration(const ConfigMap &conf) {
  if (!conf.exist("siftDataPath")) {
    throw runtime_error("Missing required configuration key: siftDataPath.");
  }
  if (!conf.exist("windowSize")) {
    throw runtime_error("Missing required configuration key: windowSize.");
  }
  if (!conf.exist("windowSlide")) {
    throw runtime_error("Missing required configuration key: windowSlide.");
  }
}

// Function to safely print basic vector information
void PrintBasicVectorInfo(const std::unique_ptr<VectorRecord>& record) {
  if (!record) {
    std::cout << "Error: Null record received" << std::endl;
    return;
  }

  std::cout << "Vector ID: " << record->uid_ 
            << ", Timestamp: " << record->timestamp_
            << ", DataType: " << static_cast<int>(record->data_.type_)
            << ", Dimension: " << record->data_.dim_ 
            << std::endl;
}

void SetupAndRunSiftPipeline(const std::string &config_file_path) {
  StreamEnvironment env;

  const auto conf = candy::StreamEnvironment::loadConfiguration(config_file_path);

  try {
    ValidateSiftConfiguration(conf);
  } catch (const exception &e) {
    throw;
  }

  PerformanceMonitor monitor;
  monitor.StartProfiling();

  try {
    // Create a SIFT stream source
    auto sift_stream = make_shared<SiftStreamSource>("SiftStream", conf.getString("siftDataPath"));
    
    // Set window parameters - using getI64 instead of getInt
    int window_size = static_cast<int>(conf.getI64("windowSize"));
    int window_slide = static_cast<int>(conf.getI64("windowSlide"));
    
    std::cout << "Window size: " << window_size << ", Window slide: " << window_slide << std::endl;
    
    // We'll collect vectors manually and process them in batches
    // This avoids relying on the window function that might be causing issues
    std::vector<std::unique_ptr<VectorRecord>> batch;
    batch.reserve(window_size);
    
    int processed_count = 0;
    int window_count = 0;
    
    // Initialize the stream source
    sift_stream->Init();
    
    std::cout << "Processing SIFT vectors in batches..." << std::endl;
    
    // Process vectors in batches manually
    while (!should_stop) {
      // Get next vector
      auto record = sift_stream->Next();
      
      // Break if no more records
      if (!record) {
        std::cout << "End of stream reached." << std::endl;
        break;
      }
      
      processed_count++;
      
      // Filter: only keep vectors with non-negative first component
      if (record->data_.type_ == DataType::Float32 && record->data_.data_) {
        const float* vector_data = reinterpret_cast<float*>(record->data_.data_.get());
        if (vector_data[0] < 0.0f) {
          continue;  // Skip this vector
        }
      }
      
      // Print basic info periodically
      if (processed_count % 1000 == 0) {
        std::cout << "Processed " << processed_count << " vectors..." << std::endl;
      }
      
      // Add to batch
      batch.push_back(std::move(record));
      
      // Process batch when we have enough vectors
      if (batch.size() >= static_cast<size_t>(window_size)) {
        window_count++;
        std::cout << "--------- Window " << window_count << " ---------" << std::endl;
        
        float total_components = 0.0f;
        int vector_count = 0;
        
        // Process all vectors in the window
        for (const auto& vec : batch) {
          if (vec && vec->data_.type_ == DataType::Float32 && vec->data_.data_) {
            // Count this as a valid vector
            vector_count++;
            
            // Sum all components in the vector
            const float* vector_data = reinterpret_cast<float*>(vec->data_.data_.get());
            for (int i = 0; i < vec->data_.dim_; i++) {
              total_components += vector_data[i];
            }
          }
        }
        
        // Calculate statistics for the window
        if (vector_count > 0) {
          std::cout << "Window contains " << vector_count << " valid vectors" << std::endl;
          std::cout << "Average across all vector components: " 
                    << (total_components / (vector_count * batch[0]->data_.dim_)) << std::endl;
          
          // Print first vector in window as sample
          if (!batch.empty() && batch[0]) {
            std::cout << "Sample vector from window:" << std::endl;
            PrintBasicVectorInfo(batch[0]);
          }
        } else {
          std::cout << "Window contains no valid vectors" << std::endl;
        }
        
        // Slide window by removing slide_size vectors from the beginning
        if (window_slide > 0 && static_cast<size_t>(window_slide) < batch.size()) {
          batch.erase(batch.begin(), batch.begin() + window_slide);
        } else {
          // Clear batch if slide size is 0 or >= batch size
          batch.clear();
        }
        
        std::cout << "--------------------------------" << std::endl;
        
        // Limit to 20 windows for testing
        if (window_count >= 20) {
          std::cout << "Reached window limit. Stopping." << std::endl;
          break;
        }
      }
    }
    
    std::cout << "Finished processing. Total vectors processed: " << processed_count << std::endl;
    
  } catch (const std::exception &e) {
    std::cerr << "Error during execution: " << e.what() << std::endl;
    throw;
  }

  monitor.StopProfiling();
}
}  // namespace candy

auto main(int argc, char *argv[]) -> int {
  const std::string default_config_file = CANDY_PATH + CONFIG_DIR + "sift_config.toml";

  string config_file_path;
  if (argc < 2) {
    config_file_path = default_config_file;
  } else {
    config_file_path = CANDY_PATH + CONFIG_DIR + string(argv[1]);
  }

  try {
    candy::SetupAndRunSiftPipeline(config_file_path);
  } catch (const exception &e) {
    cerr << "Error: " << e.what() << '\n';
    return 1;
  }

  return 0;
}