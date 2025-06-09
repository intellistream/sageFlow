// //
// // Created by Pc on 25-5-30.
// //
// // examples/Streaming/join.cpp
// #include <common/data_types.h>
// #include <compute_engine/compute_engine.h>
// #include <stream/stream_environment.h>
// #include <utils/conf_map.h>
// #include <utils/monitoring.h> // For PerformanceMonitor
//
// #include <iostream>
// #include <stdexcept>
// #include <string>
// #include <memory> // For std::make_unique, std::make_shared
//
// #include "function/join_function.h"
// #include "function/sink_function.h"
// #include "operator/join_operator.h" // Required for JoinOperator itself if not solely relying on Stream::join
// #include "stream/data_stream_source/file_stream_source.h"
// // Add other necessary includes like StorageManager, ConcurrencyManager if used directly
// #include "storage/storage_manager.h"
// #include "concurrency/concurrency_manager.h"
//
//
// using namespace std;    // NOLINT
// using namespace candy;  // NOLINT
//
// const std::string CANDY_PATH = PROJECT_DIR;
// #define CONFIG_DIR "/config/"
//
// namespace candy {
//
// // Forward declaration if SimpleStreamSource is used and not included yet.
// // class SimpleStreamSource;
//
// void ValidateJoinConfiguration(const ConfigMap &conf) {
//     if (!conf.exist("leftInputPath") || !conf.exist("rightInputPath") || !conf.exist("outputPath")) {
//         throw runtime_error("Missing required configuration keys: leftInputPath, rightInputPath, or outputPath.");
//     }
//     if (!conf.exist("joinMethod")) {
//         throw runtime_error("Missing required configuration key: joinMethod.");
//     }
//     if (!conf.exist("joinSimilarityThreshold")) {
//         throw runtime_error("Missing required configuration key: joinSimilarityThreshold.");
//     }
//     if (!conf.exist("dimension")) {
//         throw runtime_error("Missing required configuration key: dimension.");
//     }
//     if (!conf.exist("timeWindow")) {
//         throw runtime_error("Missing required configuration key: timeWindow.");
//     }
//     // Optional: Add checks for IVF params if joinMethod is IVF-based and you want them configurable here.
// }
//
// void SetupAndRunJoinPipeline(const std::string &config_file_path) {
//   StreamEnvironment env;
//
//   const auto conf = candy::StreamEnvironment::loadConfiguration(config_file_path);
//
//   try {
//     ValidateJoinConfiguration(conf);
//   } catch (const exception &e) {
//     cerr << "Configuration validation failed: " << e.what() << endl;
//     throw;
//   }
//
//   PerformanceMonitor monitor;
//   monitor.StartProfiling();
//
//   try {
//     // 1. Initialize StorageManager and ConcurrencyManager
//     auto sm = std::make_shared<candy::StorageManager>();
//     // TODO: Ensure ComputeEngine is correctly initialized and assigned if not done by SM constructor.
//     if (!sm->engine_) { // Or however your SM/CE setup works
//       sm->engine_ = std::make_shared<candy::ComputeEngine>();
//     }
//     auto cm = std::make_shared<candy::ConcurrencyManager>(sm);
//     env.setConcurrencyManager(cm); // Set CM for the environment
//
//     // 2. Create Data Sources for Left and Right Streams
//     // Ensure FileStreamSource can parse your data format (e.g., CSV: uid,timestamp,dim,v1,v2,...)
//     auto left_stream_source = make_shared<FileStreamSource>("LeftStreamSource", conf.getString("leftInputPath"));
//     auto right_stream_source = make_shared<FileStreamSource>("RightStreamSource", conf.getString("rightInputPath"));
//
//     // Add source streams to the environment
//     env.addStream(left_stream_source);
//     env.addStream(right_stream_source);
//
//     // 3. Define JoinFunction
//     int dim = conf.getInt("dimension");
//     int64_t time_window = conf.getInt64("timeWindow"); // in milliseconds
//     // Note: JoinFunction's constructor calculates step size internally as time_window / 4
//     double join_similarity_threshold_conf = conf.getDouble("joinSimilarityThreshold");
//     std::string join_method_name_conf = conf.getString("joinMethod");
//
//     // Define the actual join predicate logic
//     auto join_predicate = [engine = sm->engine_, threshold = join_similarity_threshold_conf](
//                               std::unique_ptr<candy::VectorRecord>& left_rec,
//                               std::unique_ptr<candy::VectorRecord>& right_rec) -> bool {
//       if (!left_rec || !right_rec || !left_rec->data_.data_ || !right_rec->data_.data_) {
//         // std::cerr << "Join predicate: Null record or data." << std::endl;
//         return false;
//       }
//       if (left_rec->data_.dim_ != right_rec->data_.dim_ || left_rec->data_.dim_ == 0) {
//         // std::cerr << "Join predicate: Dimension mismatch or zero dimension." << std::endl;
//         return false;
//       }
//       // TODO: Ensure your ComputeEngine::Similarity takes alpha or uses a sensible default.
//       // The current Ivf::query_for_join uses a default alpha of 1.0 in Similarity.
//       // For consistency, this predicate should ideally match that or be configurable.
//       double similarity = engine->Similarity(left_rec->data_, right_rec->data_);
//       // std::cout << "LUID: " << left_rec->uid_ << " RUID: " << right_rec->uid_ << " Sim: " << similarity << " Thr: " << threshold << std::endl;
//       return similarity > threshold;
//     };
//
//     auto join_function_instance = std::make_unique<candy::JoinFunction>(
//         "StreamJoinFunction", join_predicate, time_window, dim);
//
//     // 4. Perform the Join operation using Stream API
//     // The Stream::join method creates the JoinOperator internally.
//     // It uses the join_method_name_conf and join_similarity_threshold_conf
//     // for the created JoinOperator.
//     // IVF parameters (nlist, nprobes) for the JoinOperator are currently
//     // taken from JoinOperator's internal defaults if using IVF methods.
//     // For more control, JoinOperator constructor or Stream::join would need to accept them.
//     // TODO: Critical - Ensure Ivf constructor takes StorageManager and CM correctly creates IVF indexes.
//   } catch (const std::exception &e) {
//     cerr << "Error during join pipeline setup: " << e.what() << endl;
//     throw;
//   }
// }
// } // namespace candy
//
// auto main(int argc, char *argv[]) -> int {
//   const std::string default_config_file = CANDY_PATH + CONFIG_DIR + "join_config.toml";
//
//   string config_file_path;
//   if (argc < 2) {
//     config_file_path = default_config_file;
//   } else {
//     config_file_path = CANDY_PATH + CONFIG_DIR + string(argv[1]);
//   }
//
//   try {
//     SetupAndRunJoinPipeline(config_file_path);
//   } catch (const exception &e) {
//     cerr << "Error: " << e.what() << '\n';
//     return 1;
//   }
//
//   return 0;
// }
