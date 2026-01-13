#include "test_utils/join_test_helper.h"
#include "test_utils/data_source/vector_list_source.h"

namespace sageFlow { namespace test {

std::pair<std::vector<std::unique_ptr<VectorRecord>>,
          std::vector<std::unique_ptr<VectorRecord>>>
JoinTestHelper::generateJoinStreamsFromGenerator(
    TestDataGenerator& generator,
    bool apply_uid_offset,
    int64_t time_interval_ms) {
  
  // Generate data
  auto [records, _] = generator.generateData();
  
  // Get vectors for duplication
  auto vectors = generator.getLastGeneratedVectors();
  if (vectors.empty()) {
    throw std::runtime_error("No vectors generated from TestDataGenerator");
  }

  // Create a vector list source and use Duplicate mode
  auto source = std::make_shared<VectorListSource>(vectors);
  auto config = JoinDataSourceFactory::createDuplicated(source, apply_uid_offset);
  
  // Set time_interval from parameter or use generator's config
  if (time_interval_ms > 0) {
    config.time_interval = time_interval_ms;
  } else {
    config.time_interval = generator.getConfig().time_interval;
  }
  
  JoinDataSourcePair pair(config);
  
  return pair.generateStreams();
}

std::pair<std::vector<std::unique_ptr<VectorRecord>>,
          std::vector<std::unique_ptr<VectorRecord>>>
JoinTestHelper::generateJoinStreams(
    JoinDataSourcePair& pair,
    size_t max_records) {
  return pair.generateStreams(max_records);
}

std::pair<std::vector<std::unique_ptr<VectorRecord>>,
          std::vector<std::unique_ptr<VectorRecord>>>
JoinTestHelper::generateJoinStreamsFromSource(
    std::shared_ptr<DataSourceBase> source,
    bool apply_uid_offset,
    size_t max_records) {
  
  auto config = JoinDataSourceFactory::createDuplicated(source, apply_uid_offset);
  JoinDataSourcePair pair(config);
  return pair.generateStreams(max_records);
}

std::pair<std::vector<std::unique_ptr<VectorRecord>>,
          std::vector<std::unique_ptr<VectorRecord>>>
JoinTestHelper::generateJoinStreamsFromSeparateSources(
    std::shared_ptr<DataSourceBase> left_source,
    std::shared_ptr<DataSourceBase> right_source,
    bool apply_uid_offset,
    size_t max_records) {
  
  auto config = JoinDataSourceFactory::createSeparate(
      left_source, right_source, apply_uid_offset);
  JoinDataSourcePair pair(config);
  return pair.generateStreams(max_records);
}

std::pair<std::vector<std::unique_ptr<VectorRecord>>,
          std::vector<std::unique_ptr<VectorRecord>>>
JoinTestHelper::generatePairedJoinStreams(
    TestDataGenerator& generator,
    bool apply_uid_offset,
    int64_t time_interval_ms) {
  
  // Generate data - vectors are in pairs: [base0, perturbed0, base1, perturbed1, ...]
  auto [records, expected_matches] = generator.generateData();
  auto vectors = generator.getLastGeneratedVectors();
  
  if (vectors.empty()) {
    throw std::runtime_error("No vectors generated from TestDataGenerator");
  }
  
  // Split vectors into base (even indices) and perturbed (odd indices)
  std::vector<std::vector<float>> base_vectors;
  std::vector<std::vector<float>> perturbed_vectors;
  
  for (size_t i = 0; i < vectors.size(); i += 2) {
    base_vectors.push_back(vectors[i]);
    if (i + 1 < vectors.size()) {
      perturbed_vectors.push_back(vectors[i + 1]);
    }
  }
  
  // Create separate sources for left (base) and right (perturbed)
  auto left_source = std::make_shared<VectorListSource>(base_vectors);
  auto right_source = std::make_shared<VectorListSource>(perturbed_vectors);
  
  auto config = JoinDataSourceFactory::createSeparate(left_source, right_source, apply_uid_offset);
  
  // Set time_interval
  if (time_interval_ms > 0) {
    config.time_interval = time_interval_ms;
  } else {
    config.time_interval = generator.getConfig().time_interval;
  }
  
  JoinDataSourcePair pair(config);
  return pair.generateStreams();
}

}} // namespace sageFlow::test
