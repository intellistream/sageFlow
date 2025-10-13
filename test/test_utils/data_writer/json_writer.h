#pragma once

#include "test_utils/data_writer/data_writer_base.h"

namespace sageFlow { namespace test {

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
 *   ]
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

  std::string getFileExtension() const override { return ".json"; }
  
  std::string getFormatDescription() const override {
    return "JSON format (human-readable, good for visualization)";
  }
};

}} // namespace sageFlow::test
