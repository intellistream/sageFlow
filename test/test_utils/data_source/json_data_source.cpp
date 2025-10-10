#include "test_utils/data_source/json_data_source.h"
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <iostream>

namespace sageFlow { namespace test {

JsonDataSource::JsonDataSource(const Config& config)
    : config_(config), dimension_(0), current_index_(0) {
  loadVectors();
}

void JsonDataSource::loadVectors() {
  std::ifstream input(config_.file_path);
  if (!input.is_open()) {
    throw std::runtime_error("Cannot open file: " + config_.file_path);
  }

  // Simple JSON parsing (assumes well-formed JSON)
  std::string line;
  bool in_vectors = false;
  std::vector<float> current_vector;

  while (std::getline(input, line)) {
    // Trim whitespace
    size_t start = line.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) continue;
    line = line.substr(start);

    // Parse dimension
    if (line.find("\"dimension\"") != std::string::npos) {
      size_t colon = line.find(':');
      if (colon != std::string::npos) {
        std::string value = line.substr(colon + 1);
        size_t comma = value.find(',');
        if (comma != std::string::npos) {
          value = value.substr(0, comma);
        }
        dimension_ = std::stoi(value);
      }
    }

    // Check for vectors array start
    if (line.find("\"vectors\"") != std::string::npos) {
      in_vectors = true;
      continue;
    }

    // Parse vector data
    if (in_vectors && line.find('[') != std::string::npos && line.find(']') != std::string::npos) {
      // Extract numbers between [ and ]
      size_t start_bracket = line.find('[');
      size_t end_bracket = line.find(']');
      std::string data = line.substr(start_bracket + 1, end_bracket - start_bracket - 1);

      current_vector.clear();
      std::stringstream ss(data);
      std::string token;
      while (std::getline(ss, token, ',')) {
        try {
          float value = std::stof(token);
          current_vector.push_back(value);
        } catch (...) {
          // Skip invalid tokens
        }
      }

      if (!current_vector.empty()) {
        if (dimension_ == 0) {
          dimension_ = static_cast<int>(current_vector.size());
        } else if (static_cast<int>(current_vector.size()) != dimension_) {
          throw std::runtime_error("Inconsistent dimension in JSON file at vector " + 
                                  std::to_string(vectors_.size()));
        }
        vectors_.push_back(current_vector);
      }
    }
  }

  input.close();

  if (vectors_.empty()) {
    throw std::runtime_error("No vectors loaded from file: " + config_.file_path);
  }

  std::cout << "[JsonDataSource] Loaded " << vectors_.size() 
            << " vectors of dimension " << dimension_ 
            << " from " << config_.file_path << std::endl;
}

std::vector<float> JsonDataSource::getNextVector() {
  if (!hasMore()) {
    return std::vector<float>();
  }

  std::vector<float> result = vectors_[current_index_];
  current_index_++;

  // If looping is enabled and we reached the end, reset
  if (config_.loop && current_index_ >= vectors_.size()) {
    current_index_ = 0;
  }

  return result;
}

bool JsonDataSource::hasMore() const {
  if (config_.loop) {
    return !vectors_.empty();  // Always has more if looping
  }
  return current_index_ < vectors_.size();
}

void JsonDataSource::reset() {
  current_index_ = 0;
}

}} // namespace sageFlow::test
