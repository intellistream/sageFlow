#pragma once

#include "test_utils/data_source/data_source_base.h"
#include "test_utils/data_source/random_data_source.h"
#include "test_utils/data_source/dataset_data_source.h"
#include "test_utils/data_source/json_data_source.h"
#include "test_utils/data_source/skewed_data_source.h"
#include "test_utils/dynamic_config.h"
#include <memory>
#include <stdexcept>
#include <string>

namespace sageFlow { namespace test {

/**
 * @brief Factory for creating data sources from configuration
 */
class DataSourceFactory {
public:
  /**
   * @brief Create a data source from dynamic configuration
   * @param config Configuration containing data source settings
   * @param default_dim Default vector dimension if not specified in config
   * @param default_seed Default random seed if not specified in config
   * @return Shared pointer to created data source
   */
  static std::shared_ptr<DataSourceBase> createFromConfig(
      const DynamicConfig& config,
      int default_dim = 128,
      uint32_t default_seed = 42) {
    
    std::string type = config.get<std::string>("type", "random");
    
    if (type == "random") {
      RandomDataSource::Config ds_config;
      ds_config.vector_dim = config.get<int>("vector_dim", default_dim);
      ds_config.seed = config.get<int>("seed", static_cast<int>(default_seed));
      ds_config.max_vectors = config.get<int>("max_vectors", -1);
      return std::make_shared<RandomDataSource>(ds_config);
    }
    else if (type == "dataset") {
      DatasetDataSource::Config ds_config;
      ds_config.file_path = config.get<std::string>("file_path", "");
      if (ds_config.file_path.empty()) {
        throw std::runtime_error("Dataset data source requires 'file_path' in configuration");
      }
      ds_config.expected_dim = config.get<int>("expected_dim", default_dim);
      ds_config.loop = (config.get<int>("loop", 0) != 0);  // Convert int to bool
      return std::make_shared<DatasetDataSource>(ds_config);
    }
    else if (type == "json") {
      JsonDataSource::Config ds_config;
      ds_config.file_path = config.get<std::string>("file_path", "");
      if (ds_config.file_path.empty()) {
        throw std::runtime_error("JSON data source requires 'file_path' in configuration");
      }
      ds_config.loop = (config.get<int>("loop", 0) != 0);  // Convert int to bool
      return std::make_shared<JsonDataSource>(ds_config);
    }
    else if (type == "skewed") {
      SkewedDataSource::Config ds_config;
      ds_config.vector_dim = config.get<int>("vector_dim", default_dim);
      ds_config.seed = config.get<int>("seed", static_cast<int>(default_seed));
      ds_config.max_vectors = config.get<int>("max_vectors", -1);
      ds_config.num_clusters = config.get<int>("num_clusters", 100);
      ds_config.zipf_skew = config.get<double>("zipf_skew", 1.0);
      ds_config.cluster_spread = config.get<double>("cluster_spread", 0.05);
      return std::make_shared<SkewedDataSource>(ds_config);
    }
    else {
      throw std::runtime_error("Unknown data source type: " + type);
    }
  }
};

}} // namespace sageFlow::test
