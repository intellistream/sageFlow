#include "test_utils/join_data_source.h"
#include "test_utils/test_data_adapter.h"
#include <stdexcept>
#include <iostream>

namespace sageFlow { namespace test {

JoinDataSourcePair::JoinDataSourcePair(const JoinDataSourceConfig& config)
    : config_(config) {
  
  // Validate configuration
  if (config_.mode == JoinDataSourceConfig::Mode::Duplicate) {
    if (!config_.single_source) {
      throw std::runtime_error("Single source required for Duplicate mode");
    }
  } else if (config_.mode == JoinDataSourceConfig::Mode::Separate) {
    if (!config_.left_source || !config_.right_source) {
      throw std::runtime_error("Both left and right sources required for Separate mode");
    }
    // Verify dimensions match
    if (config_.left_source->getDimension() != config_.right_source->getDimension()) {
      throw std::runtime_error("Left and right sources must have same dimension");
    }
  }
}

std::pair<std::vector<std::unique_ptr<VectorRecord>>, 
          std::vector<std::unique_ptr<VectorRecord>>> 
JoinDataSourcePair::generateStreams(size_t max_records) {
  std::vector<std::unique_ptr<VectorRecord>> left_records;
  std::vector<std::unique_ptr<VectorRecord>> right_records;

  int64_t timestamp = config_.base_timestamp;
  size_t count = 0;

  if (config_.mode == JoinDataSourceConfig::Mode::Duplicate) {
    // Duplicate mode: generate from single source, duplicate to both sides
    auto& source = config_.single_source;
    source->reset();

    while (source->hasMore() && (max_records == 0 || count < max_records)) {
      auto vec = source->getNextVector();
      if (vec.empty()) break;

      // Create left record
      uint64_t left_uid = next_left_uid_++;
      left_records.push_back(createRecord(left_uid, vec, timestamp));

      // Create right record (possibly with UID offset)
      uint64_t right_uid = config_.apply_right_uid_offset ? 
                          (next_right_uid_++ + config_.right_uid_offset) :
                          next_right_uid_++;
      right_records.push_back(createRecord(right_uid, vec, timestamp));

      timestamp += config_.time_interval;
      count++;
    }

  } else { // Separate mode
    auto& left = config_.left_source;
    auto& right = config_.right_source;
    left->reset();
    right->reset();

    while (left->hasMore() && right->hasMore() && 
           (max_records == 0 || count < max_records)) {
      auto left_vec = left->getNextVector();
      auto right_vec = right->getNextVector();
      
      if (left_vec.empty() || right_vec.empty()) break;

      // Create left record
      uint64_t left_uid = next_left_uid_++;
      left_records.push_back(createRecord(left_uid, left_vec, timestamp));

      // Create right record (possibly with UID offset)
      uint64_t right_uid = config_.apply_right_uid_offset ? 
                          (next_right_uid_++ + config_.right_uid_offset) :
                          next_right_uid_++;
      right_records.push_back(createRecord(right_uid, right_vec, timestamp));

      timestamp += config_.time_interval;
      count++;
    }
  }

  std::cout << "[JoinDataSourcePair] Generated " << left_records.size() 
            << " left and " << right_records.size() << " right records" << std::endl;

  return {std::move(left_records), std::move(right_records)};
}

int JoinDataSourcePair::getDimension() const {
  if (config_.mode == JoinDataSourceConfig::Mode::Separate) {
    return config_.left_source->getDimension();
  } else {
    return config_.single_source->getDimension();
  }
}

int JoinDataSourcePair::getTotalCount() const {
  if (config_.mode == JoinDataSourceConfig::Mode::Separate) {
    return std::min(config_.left_source->getTotalCount(),
                   config_.right_source->getTotalCount());
  } else {
    return config_.single_source->getTotalCount();
  }
}

void JoinDataSourcePair::reset() {
  next_left_uid_ = 1;
  next_right_uid_ = 1;
  
  if (config_.mode == JoinDataSourceConfig::Mode::Separate) {
    config_.left_source->reset();
    config_.right_source->reset();
  } else {
    config_.single_source->reset();
  }
}

std::unique_ptr<VectorRecord> JoinDataSourcePair::createRecord(
    uint64_t uid, const std::vector<float>& data, int64_t timestamp) {
  auto record = createVectorRecord(uid, timestamp, data);
  TestRecordSideManager::instance().setSide(uid, (uid % 2 == 0) ? Side::LEFT : Side::RIGHT);
  return record;
}

// Factory methods

JoinDataSourceConfig JoinDataSourceFactory::createDuplicated(
    std::shared_ptr<DataSourceBase> source,
    bool apply_uid_offset) {
  JoinDataSourceConfig config;
  config.mode = JoinDataSourceConfig::Mode::Duplicate;
  config.single_source = source;
  config.apply_right_uid_offset = apply_uid_offset;
  return config;
}

JoinDataSourceConfig JoinDataSourceFactory::createSeparate(
    std::shared_ptr<DataSourceBase> left_source,
    std::shared_ptr<DataSourceBase> right_source,
    bool apply_uid_offset) {
  JoinDataSourceConfig config;
  config.mode = JoinDataSourceConfig::Mode::Separate;
  config.left_source = left_source;
  config.right_source = right_source;
  config.apply_right_uid_offset = apply_uid_offset;
  return config;
}

}} // namespace sageFlow::test
