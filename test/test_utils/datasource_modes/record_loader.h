#pragma once

#include <memory>
#include <vector>

#include "common/data_types.h"
#include "test_utils/data_source/dataset_data_source.h"
#include "test_utils/datasource_modes/config.h"

namespace sageFlow {
namespace test {

struct DatasourceRecordLoadResult {
  std::vector<std::unique_ptr<VectorRecord>> records;
  std::shared_ptr<DatasetDataSource> dataset_source_for_cache;
  bool enable_dataset_ground_truth_cache{true};
};

DatasourceRecordLoadResult loadDatasourceModeRecords(
    const DataSourceModeConfig& mode_config,
    int data_size);

}  // namespace test
}  // namespace sageFlow
