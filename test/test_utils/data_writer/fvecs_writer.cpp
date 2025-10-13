#include "test_utils/data_writer/fvecs_writer.h"
#include "utils/logger.h"
#include <fstream>
#include <stdexcept>

namespace sageFlow { namespace test {

bool FvecsWriter::writeVectors(const std::string& file_path, 
                               const std::vector<std::vector<float>>& vectors,
                               int dimension) {
  if (vectors.empty()) {
    SAGEFLOW_LOG_ERROR("TEST", "[FvecsWriter] Error: No vectors to write");
    return false;
  }

  // Validate all vectors have the correct dimension
  for (size_t i = 0; i < vectors.size(); ++i) {
    if (static_cast<int>(vectors[i].size()) != dimension) {
      SAGEFLOW_LOG_ERROR("TEST", "[FvecsWriter] Error: Vector {} has dimension {}, expected {}", 
                         i, vectors[i].size(), dimension);
      return false;
    }
  }

  std::ofstream output(file_path, std::ios::binary);
  if (!output.is_open()) {
    SAGEFLOW_LOG_ERROR("TEST", "[FvecsWriter] Error: Cannot open file for writing: {}", file_path);
    return false;
  }

  try {
    int32_t dim = static_cast<int32_t>(dimension);
    
    for (const auto& vec : vectors) {
      // Write dimension
      output.write(reinterpret_cast<const char*>(&dim), sizeof(int32_t));
      
      // Write vector data
      output.write(reinterpret_cast<const char*>(vec.data()), dim * sizeof(float));
      
      if (!output.good()) {
        SAGEFLOW_LOG_ERROR("TEST", "[FvecsWriter] Error: Write failed");
        output.close();
        return false;
      }
    }

    output.close();
    SAGEFLOW_LOG_INFO("TEST", "[FvecsWriter] Successfully wrote {} vectors of dimension {} to {}", 
                      vectors.size(), dimension, file_path);
    return true;

  } catch (const std::exception& e) {
    SAGEFLOW_LOG_ERROR("TEST", "[FvecsWriter] Exception during write: {}", e.what());
    output.close();
    return false;
  }
}

}} // namespace sageFlow::test
