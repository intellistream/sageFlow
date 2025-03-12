#include <core/common/data_types.h>
#include <core/compute_engine/compute_engine.h>
#include <core/utils/conf_map.h>
#include <core/utils/monitoring.h>
#include <streaming/stream_environment.h>

#include <iostream>
#include <stdexcept>
#include <string>

#include "runtime/function/filter_function.h"
#include "runtime/function/map_function.h"
#include "runtime/function/sink_function.h"
#include "streaming/data_stream_source/file_stream_source.h"

using namespace std;    // NOLINT
using namespace candy;  // NOLINT

const std::string CANDY_PATH = PROJECT_DIR;
#define CONFIG_DIR "/config/"

namespace candy {
void ValidateConfiguration(const ConfigMap &conf) {
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
    ValidateConfiguration(conf);
  } catch (const exception &e) {
    throw;
  }

  PerformanceMonitor monitor;
  monitor.StartProfiling();

  try {
    auto file_stream = make_shared<FileStreamSource>("FileStream", conf.getString("inputPath"));
    auto join_stream = make_shared<FileStreamSource>("JoinStream", conf.getString("inputPath"));
    file_stream
        ->filter(std::make_unique<FilterFunction>("filter1",
                                                  [](std::unique_ptr<VectorRecord> &record) -> bool {
                                                    const auto rec = record.get();
                                                    return (rec != nullptr) && rec->data_ && !rec->data_->empty() &&
                                                           (*rec->data_)[0] > 0.5;  // Filter by first value
                                                                                    // multi-branch
                                                  }))
        ->map(std::make_unique<MapFunction>(
            "map1", [](std::unique_ptr<VectorRecord> &record) { record = ComputeEngine::normalizeVector(record); }))
        ->join(join_stream, std::make_unique<JoinFunction>(
                                "join1", [](std::unique_ptr<VectorRecord> &l,
                                            std::unique_ptr<VectorRecord> &r) { return l->id_ == r->id_; })

                   )
        ->writeSink(std::make_unique<SinkFunction>(
            "sink1",
            [](const std::unique_ptr<VectorRecord> &record) { std::cout << "Sink1: " << record->id_ << '\n'; }))
        ->writeSink(std::make_unique<SinkFunction>("sink2", [](const std::unique_ptr<VectorRecord> &record) {
          std::cout << "Sink2: " << (*record->data_)[0] << '\n';
        }));
    env.addStream(std::move(file_stream));
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
    SetupAndRunPipeline(config_file_path);
  } catch (const exception &e) {
    cerr << "Error: " << e.what() << '\n';
    return 1;
  }

  return 0;
}
