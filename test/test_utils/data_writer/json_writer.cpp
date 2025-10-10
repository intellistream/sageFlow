#include "test_utils/data_writer/json_writer.h"
#include <fstream>
#include <iomanip>
#include <iostream>

namespace sageFlow { namespace test {

bool JsonWriter::writeVectors(const std::string& file_path, 
                              const std::vector<std::vector<float>>& vectors,
                              int dimension) {
  if (vectors.empty()) {
    std::cerr << "[JsonWriter] Error: No vectors to write" << std::endl;
    return false;
  }

  // Validate all vectors have the correct dimension
  for (size_t i = 0; i < vectors.size(); ++i) {
    if (static_cast<int>(vectors[i].size()) != dimension) {
      std::cerr << "[JsonWriter] Error: Vector " << i << " has dimension " 
                << vectors[i].size() << ", expected " << dimension << std::endl;
      return false;
    }
  }

  std::ofstream output(file_path);
  if (!output.is_open()) {
    std::cerr << "[JsonWriter] Error: Cannot open file for writing: " << file_path << std::endl;
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
    std::cout << "[JsonWriter] Successfully wrote " << vectors.size() 
              << " vectors of dimension " << dimension 
              << " to " << file_path << std::endl;
    return true;

  } catch (const std::exception& e) {
    std::cerr << "[JsonWriter] Exception during write: " << e.what() << std::endl;
    output.close();
    return false;
  }
}

}} // namespace sageFlow::test
