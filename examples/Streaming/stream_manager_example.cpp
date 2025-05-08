#include <common/data_types.h>
#include <stream/stream_environment.h>
#include <utils/monitoring.h>
#include <iostream>
#include <stdexcept>
#include <string>
#include "function/filter_function.h"
#include "function/map_function.h"
#include "function/sink_function.h"
#include "stream/data_stream_source/file_stream_source.h"

using namespace std;    // NOLINT
using namespace candy;  // NOLINT

const std::string CANDY_PATH = PROJECT_DIR;
#define CONFIG_DIR "/config/"

namespace candy {
void ValidateConfiguration(const ConfigMap &conf) {
  if (!conf.exist("inputPath")) {
    throw runtime_error("Missing required configuration key: inputPath.");
  }
}

void SetupAndRunPipeline(const std::string &config_file_path) {
  StreamEnvironment env;

  const auto conf = candy::StreamEnvironment::loadConfiguration(config_file_path);

  try {
    ValidateConfiguration(conf);
  } catch (const exception &e) {
    throw;
  }

  PerformanceMonitor monitor;
  monitor.StartProfiling();

  try {
    // Ensure all stream operations are managed through StreamEnvironment
    auto file_stream = make_shared<FileStreamSource>("FileStream", conf.getString("inputPath"));

    // Apply a filter function
    auto filtered_stream = file_stream->filter(std::make_unique<FilterFunction>("filter1",
        [](const std::unique_ptr<VectorRecord>& record) -> bool {
            return record && record->timestamp_ % 2 == 0; // Example: filter even timestamps
        }));

    // Ensure all operations use the filtered stream
    auto mapped_stream = filtered_stream->map(std::make_unique<MapFunction>("map1",
        [](std::unique_ptr<VectorRecord>& record) -> std::unique_ptr<VectorRecord> {
            if (record) {
                record->timestamp_ += 1000; // Example: add 1000 to the timestamp
            }
            return std::move(record);
        }));

    // Write results to a sink
    mapped_stream->writeSink(std::make_unique<SinkFunction>("sink1",
        [](const std::unique_ptr<VectorRecord>& record) {
            if (record) {
                std::cout << "Sink received record with timestamp: " << record->timestamp_ << '\n';
            }
        }));

    // Register the filtered stream with the environment
    env.addStream(filtered_stream);

    env.execute();
  } catch (const std::exception &e) {
    throw;
  }

  monitor.StopProfiling();
}
}  // namespace candy

auto main(int argc, char *argv[]) -> int {
  const std::string default_config_file = CANDY_PATH + CONFIG_DIR + "default_config.toml";

  string config_file_path;
  if (argc < 2) {
    config_file_path = default_config_file;
  } else {
    config_file_path = CANDY_PATH + CONFIG_DIR + string(argv[1]);
  }

  try {
    candy::SetupAndRunPipeline(config_file_path);
  } catch (const exception &e) {
    cerr << "Error: " << e.what() << '\n';
    return 1;
  }

  return 0;
}