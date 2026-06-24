#include "operator/join_operator_components/join_partitioner_factory.h"

#include "execution/centroid_partitioner.h"
#include "utils/logger.h"

namespace sageFlow {

namespace {

CentroidPartitioner::Config makeCentroidConfig(
    const JoinStrategyConfig& config,
    int dimension,
    int num_partitions,
    int fallback_partitions) {
  CentroidPartitioner::Config cp_config;
  cp_config.num_partitions = (num_partitions > 0) ? num_partitions : fallback_partitions;
  cp_config.overlap_ratio = config.clustered_overlap_ratio;
  cp_config.dimension = (dimension > 0) ? dimension : config.dimension;
  cp_config.seed = 42;
  cp_config.rebalance_threshold = config.clustered_rebalance_threshold;
  cp_config.multicast_k = config.clustered_multicast_k;
  cp_config.training_samples = static_cast<size_t>(config.clustered_training_samples);
  cp_config.enable_cold_start = config.enable_cold_start;
  return cp_config;
}

}  // namespace

std::unique_ptr<IPartitioner> JoinPartitionerFactory::createPreferred(
    const JoinStrategyConfig& config,
    bool use_strategy_config,
    int dimension,
    int num_partitions) {
  if (!use_strategy_config) {
    return nullptr;
  }

  switch (config.algorithm) {
    case JoinAlgorithm::CLUSTERED_JOIN: {
      auto cp_config = makeCentroidConfig(
          config, dimension, num_partitions, config.num_partitions);
      auto partitioner = std::make_unique<CentroidPartitioner>(cp_config);
      partitioner->setMulticastEnabled(config.clustered_multicast_enabled);

      SAGEFLOW_LOG_INFO("JOIN",
                        "Created CentroidPartitioner for ClusteredJoin: "
                        "partitions={} overlap={:.2f} multicast={} multicast_k={} "
                        "training_samples={} cold_start={}",
                        cp_config.num_partitions,
                        cp_config.overlap_ratio,
                        config.clustered_multicast_enabled,
                        cp_config.multicast_k,
                        cp_config.training_samples,
                        cp_config.enable_cold_start);

      return partitioner;
    }

    case JoinAlgorithm::S3J: {
      CentroidPartitioner::Config cp_config;
      cp_config.num_partitions = (num_partitions > 0)
          ? num_partitions
          : config.s3j_num_centroids;
      cp_config.overlap_ratio = config.clustered_overlap_ratio;
      cp_config.dimension = (dimension > 0) ? dimension : config.dimension;
      cp_config.seed = 42;
      return std::make_unique<CentroidPartitioner>(cp_config);
    }

    case JoinAlgorithm::VSJOIN: {
      // Keep the existing behavior: VSJoin temporarily reuses CentroidPartitioner
      // for multicast support until the LSH adapter exposes partitionMulti().
      auto cp_config = makeCentroidConfig(
          config, dimension, num_partitions, config.num_partitions);
      auto partitioner = std::make_unique<CentroidPartitioner>(cp_config);
      partitioner->setMulticastEnabled(config.clustered_multicast_enabled);
      return partitioner;
    }

    case JoinAlgorithm::BRUTEFORCE:
    case JoinAlgorithm::IVF:
    case JoinAlgorithm::HNSW:
    case JoinAlgorithm::HDR_TREE:
    default:
      return nullptr;
  }
}

}  // namespace sageFlow
