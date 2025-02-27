#include <core/common/data_types.h>
#include <core/compute_engine/compute_engine.h>
#include <core/utils/conf_map.h>
#include <core/utils/monitoring.h>
#include <runtime/operators/log_operator.h>
#include <streaming/stream_environment.h>
#include <vector_db/vector_database.h>

#include <iostream>
#include <stdexcept>
#include <string>

using namespace std; // no-lint
using namespace candy; // no-lint

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

void SetupAndRunPipeline(const std::string &configFilePath) {
  StreamEnvironment env;

  const auto conf = env.loadConfiguration(configFilePath);

  try {
    ValidateConfiguration(conf);
  } catch (const exception &e) {
    throw;
  }

  PerformanceMonitor monitor;
  monitor.StartProfiling();

  try {
    auto source_stream = env.readSource("VectorSource", conf.getString("inputPath"));

    auto t =
        source_stream
            ->filter([](const std::shared_ptr<VectorRecord> &record) -> bool {
              std::cout << "Filter: " << record->id_ << std::endl;
              return record && record->data_ && !record->data_->empty() &&
                     (*record->data_)[0] > 0.5;  // Filter by first value
            })
            ->map(
              [](const std::shared_ptr<VectorRecord> &record) {
              std::cout << "Map: " << record->id_ << std::endl;
              return ComputeEngine::normalizeVector(record);
            })
            ->join(
              env.readSource("OtherSource", conf.getString("inputPath")),
                   [&](const std::shared_ptr<VectorRecord> &left, const std::shared_ptr<VectorRecord> &right) {
                     return ComputeEngine::calculateSimilarity(left, right) > conf.getDouble("similarityThreshold");
                  }
                  );
    t->processStream();
  } catch (const std::exception &e) {
    throw;
  }

  monitor.StopProfiling();
}
}  // namespace candy

auto main(int argc, char *argv[]) -> int {
  const std::string default_config_file = CANDY_PATH + CONFIG_DIR + "default_config.txt";

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
