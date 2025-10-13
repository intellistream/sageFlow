#pragma once

#include "test_utils/data_writer/data_writer_base.h"

namespace sageFlow { namespace test {

/**
 * @brief Writer for fvecs binary format
 * 
 * fvecs format specification:
 * - Each vector is stored as: [dimension(int32)] [vector_data(float32 * dimension)]
 * - This is the standard format used in vector search benchmarks (SIFT, GIST, etc.)
 * - Binary format, efficient for large datasets
 */
class FvecsWriter : public DataWriterBase {
public:
  FvecsWriter() = default;

  bool writeVectors(const std::string& file_path, 
                   const std::vector<std::vector<float>>& vectors,
                   int dimension) override;

  std::string getFileExtension() const override { return ".fvecs"; }
  
  std::string getFormatDescription() const override {
    return "FVECS binary format (dimension + float data per vector)";
  }
};

}} // namespace sageFlow::test
