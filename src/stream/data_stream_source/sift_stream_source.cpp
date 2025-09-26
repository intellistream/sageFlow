//
// Created on 25-5-6.
//
#include "stream/data_stream_source/sift_stream_source.h"

#include <fstream>
#include <iostream>
#include <utility>
#include "utils/logger.h"

candy::SiftStreamSource::SiftStreamSource(std::string name)
    : DataStreamSource(std::move(name), DataStreamSourceType::None) {}

candy::SiftStreamSource::SiftStreamSource(std::string name, std::string file_path)
    : DataStreamSource(std::move(name), DataStreamSourceType::None), file_path_(std::move(file_path)) {}

void candy::SiftStreamSource::Init() {
  std::ifstream file(file_path_, std::ios::binary);
  if (!file.is_open()) {
    CANDY_LOG_ERROR("SOURCE", "open_fail path={} ", file_path_);
    return;
  }

  // Process the .fvecs file
  while (file.good()) {
    // Read vector dimension (first int in each vector record)
    int32_t dimension;
    file.read(reinterpret_cast<char*>(&dimension), sizeof(int32_t));
    
    if (!file.good()) {
      break;  // End of file or error
    }

    // Allocate memory for the float vector data
    auto* vector_data = new float[dimension];
    file.read(reinterpret_cast<char*>(vector_data), dimension * sizeof(float));
    
    if (!file.good() && !file.eof()) {
      // Error reading the vector data
      delete[] vector_data;
      CANDY_LOG_ERROR("SOURCE", "read_vector_fail path={} index={} ", file_path_, records_.size());
      break;
    }

    // Create a VectorRecord
    // Convert float array to char* for VectorRecord constructor
    char* data_ptr = reinterpret_cast<char*>(vector_data);

    // Generate unique ID for each vector (using its position in the file)
    uint64_t uid = records_.size();
    int64_t timestamp = static_cast<int64_t>(records_.size());  // Use position as timestamp too

    auto record = std::make_unique<VectorRecord>(uid, timestamp, dimension, DataType::Float32, data_ptr);
    records_.push_back(std::move(record));
    
    // Note: The VectorRecord takes ownership of the allocated memory
  }

  file.close();
  CANDY_LOG_INFO("SOURCE", "sift_loaded count={} path={} ", records_.size(), file_path_);
}

auto candy::SiftStreamSource::Next() -> std::unique_ptr<VectorRecord> {
  if (records_.empty()) {
    return nullptr;
  }
  auto record = std::move(records_.back());
  records_.pop_back();
  return record;
}