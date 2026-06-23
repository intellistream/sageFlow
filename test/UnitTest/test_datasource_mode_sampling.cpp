#include <gtest/gtest.h>

#include <stdexcept>
#include <vector>

#include "test_utils/datasource_modes/dataset_sampling.h"

namespace sageFlow {
namespace test {

TEST(DatasourceModeSamplingTest, SequentialUsesOffsetAndStopsWhenLoopDisabled) {
  DataSourceModeConfig config;
  config.data_source_sample_mode = "sequential";
  config.data_source_sample_offset = 2;
  config.data_source_loop = false;

  const auto indices = buildDatasetSampleIndices(5, 10, config);

  EXPECT_EQ(indices, (std::vector<size_t>{2, 3, 4}));
}

TEST(DatasourceModeSamplingTest, SequentialLoopsFromOffset) {
  DataSourceModeConfig config;
  config.data_source_sample_mode = "sequential";
  config.data_source_sample_offset = 3;
  config.data_source_loop = true;

  const auto indices = buildDatasetSampleIndices(5, 5, config);

  EXPECT_EQ(indices, (std::vector<size_t>{3, 4, 0, 1, 2}));
}

TEST(DatasourceModeSamplingTest, StrideUsesOffsetAndStride) {
  DataSourceModeConfig config;
  config.data_source_sample_mode = "stride";
  config.data_source_sample_offset = 1;
  config.data_source_sample_stride = 3;
  config.data_source_loop = false;

  const auto indices = buildDatasetSampleIndices(10, 4, config);

  EXPECT_EQ(indices, (std::vector<size_t>{1, 4, 7}));
}

TEST(DatasourceModeSamplingTest, RandomSamplingIsDeterministic) {
  DataSourceModeConfig config;
  config.data_source_sample_mode = "random";
  config.data_source_sample_seed = 17;
  config.data_source_loop = false;

  const auto first = buildDatasetSampleIndices(20, 8, config);
  const auto second = buildDatasetSampleIndices(20, 8, config);

  EXPECT_EQ(first, second);
  EXPECT_EQ(first.size(), 8U);
}

TEST(DatasourceModeSamplingTest, ExhaustedDatasetReturnsEmptyWhenLoopDisabled) {
  DataSourceModeConfig config;
  config.data_source_sample_mode = "sequential";
  config.data_source_sample_offset = 10;
  config.data_source_loop = false;

  const auto indices = buildDatasetSampleIndices(5, 3, config);

  EXPECT_TRUE(indices.empty());
}

TEST(DatasourceModeSamplingTest, UnknownModeFailsEarly) {
  DataSourceModeConfig config;
  config.data_source_sample_mode = "unknown";

  EXPECT_THROW((void)buildDatasetSampleIndices(5, 3, config), std::runtime_error);
}

}  // namespace test
}  // namespace sageFlow
