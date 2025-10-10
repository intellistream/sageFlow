#include "test_utils/data_writer/fvecs_writer.h"
#include <fstream>
#include <stdexcept>
#include <iostream>

namespace sageFlow { namespace test {

bool FvecsWriter::writeVectors(const std::string& file_path, 
                               const std::vector<std::vector<float>>& vectors,
                               int dimension) {
  if (vectors.empty()) {
    std::cerr << "[FvecsWriter] Error: No vectors to write" << std::endl;
    return false;
  }

  // Validate all vectors have the correct dimension
  for (size_t i = 0; i < vectors.size(); ++i) {
    if (static_cast<int>(vectors[i].size()) != dimension) {
      std::cerr << "[FvecsWriter] Error: Vector " << i << " has dimension " 
                << vectors[i].size() << ", expected " << dimension << std::endl;
      return false;
    }
  }

  std::ofstream output(file_path, std::ios::binary);
  if (!output.is_open()) {
    std::cerr << "[FvecsWriter] Error: Cannot open file for writing: " << file_path << std::endl;
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
        std::cerr << "[FvecsWriter] Error: Write failed" << std::endl;
        output.close();
        return false;
      }
    }

    output.close();
    std::cout << "[FvecsWriter] Successfully wrote " << vectors.size() 
              << " vectors of dimension " << dimension 
              << " to " << file_path << std::endl;
    return true;

  } catch (const std::exception& e) {
    std::cerr << "[FvecsWriter] Exception during write: " << e.what() << std::endl;
    output.close();
    return false;
  }
}

}} // namespace sageFlow::test
