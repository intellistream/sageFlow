#include "test_utils/data_writer/json_writer.h"
#include "utils/logger.h"
#include <fstream>
#include <iomanip>

namespace sageFlow { namespace test {

bool JsonWriter::writeVectors(const std::string& file_path, 
                              const std::vector<std::vector<float>>& vectors,
                              int dimension) {
  if (vectors.empty()) {
    SAGEFLOW_LOG_ERROR("TEST", "[JsonWriter] Error: No vectors to write");
    return false;
  }

  // Validate all vectors have the correct dimension
  for (size_t i = 0; i < vectors.size(); ++i) {
    if (static_cast<int>(vectors[i].size()) != dimension) {
      SAGEFLOW_LOG_ERROR("TEST", "[JsonWriter] Error: Vector {} has dimension {}, expected {}", 
                         i, vectors[i].size(), dimension);
      return false;
    }
  }

  std::ofstream output(file_path);
  if (!output.is_open()) {
    SAGEFLOW_LOG_ERROR("TEST", "[JsonWriter] Error: Cannot open file for writing: {}", file_path);
    return false;
  }

  try {
    output << std::fixed << std::setprecision(6);
    
    // Write JSON header
    output << "{\n";
    output << "  \"dimension\": " << dimension << ",\n";
    output << "  \"count\": " << vectors.size() << ",\n";
    output << "  \"vectors\": [\n";

    // Write vectors
    for (size_t i = 0; i < vectors.size(); ++i) {
      output << "    [";
      for (size_t j = 0; j < vectors[i].size(); ++j) {
        output << vectors[i][j];
        if (j < vectors[i].size() - 1) {
          output << ", ";
        }
      }
      output << "]";
      if (i < vectors.size() - 1) {
        output << ",";
      }
      output << "\n";
    }

    output << "  ]\n";
    output << "}\n";

    output.close();
    SAGEFLOW_LOG_INFO("TEST", "[JsonWriter] Successfully wrote {} vectors of dimension {} to {}", 
                      vectors.size(), dimension, file_path);
    return true;

  } catch (const std::exception& e) {
    SAGEFLOW_LOG_ERROR("TEST", "[JsonWriter] Exception during write: {}", e.what());
    output.close();
    return false;
  }
}

}} // namespace sageFlow::test
