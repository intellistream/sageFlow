#pragma once

#include "test_utils/data_writer/data_writer_base.h"

namespace sageFlow { namespace test {

/**
 * @brief Supplemental metadata describing ground-truth join pairs that belong
 *        to a specific dataset under a given configuration.
 */
struct JsonGroundTruthEntry {
  std::string label;
  uint64_t window_ms = 0;
  double similarity_threshold = 0.0;
  double alpha = 0.1;
  uint64_t modulo_base = 1000000ULL;
  size_t record_count = 0;
  std::vector<std::pair<uint64_t, uint64_t>> pairs;
};

/**
 * @brief Writer for JSON format
 * 
 * JSON format for easy visualization and debugging:
 * {
 *   "dimension": 128,
 *   "count": 1000,
 *   "vectors": [
 *     [0.1, 0.2, ...],
 *     [0.3, 0.4, ...],
 *     ...
 *   ],
 *   "ground_truth_sets": [ ... ]
 * }
 * 
 * - Human-readable text format
 * - Easy to inspect and visualize
 * - Less efficient for large datasets but good for debugging
 */
class JsonWriter : public DataWriterBase {
public:
  JsonWriter() = default;

  bool writeVectors(const std::string& file_path, 
                   const std::vector<std::vector<float>>& vectors,
                   int dimension) override;

  void setGroundTruthEntries(std::vector<JsonGroundTruthEntry> entries) {
    ground_truth_entries_ = std::move(entries);
  }

  std::string getFileExtension() const override { return ".json"; }
  
  std::string getFormatDescription() const override {
    return "JSON format (human-readable, good for visualization)";
  }

private:
  std::vector<JsonGroundTruthEntry> ground_truth_entries_;
};

}} // namespace sageFlow::test
