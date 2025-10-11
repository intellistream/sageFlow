#include "test_utils/data_source/dataset_data_source.h"
#include "utils/logger.h"
#include <fstream>
#include <stdexcept>

namespace sageFlow { namespace test {

DatasetDataSource::DatasetDataSource(const Config& config)
    : config_(config), dimension_(0), current_index_(0) {
  loadVectors();
}

void DatasetDataSource::loadVectors() {
  std::ifstream input(config_.file_path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("Cannot open file: " + config_.file_path);
  }

  while (true) {
    // Read dimension for the current vector
    int32_t current_dim = 0;
    input.read(reinterpret_cast<char*>(&current_dim), sizeof(int32_t));

    if (input.eof()) {
      break;  // End of file reached cleanly
    }
    if (input.fail()) {
      throw std::runtime_error("Error reading dimension from file: " + config_.file_path);
    }

    // Check dimension consistency
    if (vectors_.empty()) {
      dimension_ = current_dim;
      if (config_.expected_dim != -1 && dimension_ != config_.expected_dim) {
        throw std::runtime_error("Unexpected dimension in file " + config_.file_path +
                                ". Expected " + std::to_string(config_.expected_dim) +
                                ", got " + std::to_string(dimension_));
      }
      if (dimension_ <= 0) {
        throw std::runtime_error("Invalid dimension read from file: " + std::to_string(dimension_));
      }
    } else if (current_dim != dimension_) {
      throw std::runtime_error("Inconsistent dimension found in file " + config_.file_path +
                              ". Expected " + std::to_string(dimension_) +
                              ", found " + std::to_string(current_dim) +
                              " at vector index " + std::to_string(vectors_.size()));
    }

    // Read vector data
    std::vector<float> vec(dimension_);
    input.read(reinterpret_cast<char*>(vec.data()), dimension_ * sizeof(float));
    if (input.fail()) {
      throw std::runtime_error("Error reading vector data from file: " + config_.file_path +
                              " at vector index " + std::to_string(vectors_.size()));
    }

    vectors_.push_back(std::move(vec));
  }

  input.close();

  if (vectors_.empty()) {
    throw std::runtime_error("No vectors loaded from file: " + config_.file_path);
  }

  SAGEFLOW_LOG_INFO("TEST", "[DatasetDataSource] Loaded {} vectors of dimension {} from {}", 
                    vectors_.size(), dimension_, config_.file_path);
}

std::vector<float> DatasetDataSource::getNextVector() {
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

bool DatasetDataSource::hasMore() const {
  if (config_.loop) {
    return !vectors_.empty();  // Always has more if looping
  }
  return current_index_ < vectors_.size();
}

void DatasetDataSource::reset() {
  current_index_ = 0;
}

}} // namespace sageFlow::test
