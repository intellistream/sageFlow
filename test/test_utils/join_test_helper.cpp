#include "test_utils/join_test_helper.h"
#include "test_utils/data_source/vector_list_source.h"

namespace sageFlow { namespace test {

std::pair<std::vector<std::unique_ptr<VectorRecord>>,
          std::vector<std::unique_ptr<VectorRecord>>>
JoinTestHelper::generateJoinStreamsFromGenerator(
    TestDataGenerator& generator,
    bool apply_uid_offset) {
  
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

}} // namespace sageFlow::test
