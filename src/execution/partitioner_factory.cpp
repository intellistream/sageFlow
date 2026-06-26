#include "execution/partitioner_factory.h"

#include "execution/centroid_partitioner.h"
#include "execution/vector_space_partitioner.h"
#include "utils/logger.h"

#include <stdexcept>

namespace sageFlow {

// =============================================================================
// LSHIPartitioner Implementation
// =============================================================================

LSHIPartitioner::LSHIPartitioner(int dimension, int num_hash_functions,
                                 int num_partitions, int seed,
                                 double boundary_threshold)
    : num_partitions_(num_partitions) {
  lsh_partitioner_ = std::make_unique<LSHPartitioner>(
      dimension, num_hash_functions, seed, boundary_threshold);
}

size_t LSHIPartitioner::partition(const Response& data, size_t num_channels) {
  const VectorRecord* record = getPartitionRecord(data);
  if (!record) {
    return 0;
  }

  // 使用内部 LSHPartitioner 进行分区
  // 注意：num_channels 可能与 num_partitions_ 不同，
  // 这里使用 num_channels 以支持动态调整
  return lsh_partitioner_->partition(*record, num_channels);
}

std::vector<size_t> LSHIPartitioner::getCandidatePartitions(
    const Response& data, size_t num_channels, size_t num_probes) const {
  const VectorRecord* record = getPartitionRecord(data);
  if (!record) {
    return {0};
  }

  return lsh_partitioner_->getCandidatePartitions(*record, num_channels,
                                                   num_probes);
}

bool LSHIPartitioner::isBoundaryVector(const Response& data,
                                        size_t num_channels) const {
  const VectorRecord* record = getPartitionRecord(data);
  if (!record) {
    return false;
  }

  return lsh_partitioner_->isBoundaryVector(*record, num_channels);
}

const LSHPartitioner* LSHIPartitioner::getLSHPartitioner() const {
  return lsh_partitioner_.get();
}

// =============================================================================
// PartitionerFactory Implementation
// =============================================================================

std::unique_ptr<IPartitioner> PartitionerFactory::create(
    PartitionStrategy strategy, int dimension, int num_partitions,
    const JoinStrategyConfig& config) {
  switch (strategy) {
    case PartitionStrategy::ROUND_ROBIN:
      SAGEFLOW_LOG_DEBUG("PartitionerFactory",
                         "Creating RoundRobinPartitioner");
      return std::make_unique<RoundRobinPartitioner>();

    case PartitionStrategy::KEY_HASH:
      SAGEFLOW_LOG_DEBUG("PartitionerFactory", "Creating KeyPartitioner");
      return std::make_unique<KeyPartitioner>();

    case PartitionStrategy::VECTOR_HASH:
      SAGEFLOW_LOG_DEBUG("PartitionerFactory",
                         "Creating VectorHashPartitioner");
      return std::make_unique<VectorHashPartitioner>();

    case PartitionStrategy::LSH: {
      SAGEFLOW_LOG_DEBUG("PartitionerFactory",
                         "Creating LSHIPartitioner with {} hash functions, "
                         "{} partitions, dimension {}",
                         config.vsjoin_num_hash_functions, num_partitions,
                         dimension);
      return std::make_unique<LSHIPartitioner>(
          dimension, config.vsjoin_num_hash_functions, num_partitions,
          42,  // seed
          config.vsjoin_boundary_threshold);
    }

    case PartitionStrategy::CENTROID: {
      SAGEFLOW_LOG_DEBUG("PartitionerFactory",
                         "Creating CentroidPartitioner with {} partitions, "
                         "dimension {}",
                         num_partitions, dimension);
      CentroidPartitioner::Config centroid_config;
      centroid_config.num_partitions = num_partitions;
      centroid_config.dimension = dimension;
      centroid_config.overlap_ratio = config.clustered_overlap_ratio;
      centroid_config.rebalance_threshold = config.clustered_rebalance_threshold;
      return std::make_unique<CentroidPartitioner>(centroid_config);
    }

    default:
      throw std::runtime_error(
          "PartitionerFactory: Unknown partition strategy: " +
          toString(strategy));
  }
}

std::unique_ptr<IPartitioner> PartitionerFactory::create(
    const JoinStrategyConfig& config) {
  return create(config.partition_strategy, config.dimension,
                config.num_partitions, config);
}

int PartitionerFactory::getRecommendedPartitionCount(PartitionStrategy strategy,
                                                     int parallelism) {
  switch (strategy) {
    case PartitionStrategy::ROUND_ROBIN:
    case PartitionStrategy::KEY_HASH:
      // 这些策略通常不需要特定的分区数，使用并行度
      return parallelism;

    case PartitionStrategy::VECTOR_HASH:
      // 向量哈希分区可以使用更多分区以获得更好的分布
      return parallelism * 2;

    case PartitionStrategy::LSH:
      // LSH 分区数应该是 2 的幂次方，便于哈希映射
      {
        int power = 1;
        while (power < parallelism) {
          power *= 2;
        }
        return power;
      }

    case PartitionStrategy::CENTROID:
      // 质心分区数应适中，过多会增加训练开销
      return std::max(parallelism, 8);

    default:
      return parallelism;
  }
}

bool PartitionerFactory::requiresTraining(PartitionStrategy strategy) {
  switch (strategy) {
    case PartitionStrategy::CENTROID:
      // 质心分区器需要训练
      return true;

    case PartitionStrategy::ROUND_ROBIN:
    case PartitionStrategy::KEY_HASH:
    case PartitionStrategy::VECTOR_HASH:
    case PartitionStrategy::LSH:
      // 这些策略不需要训练
      return false;

    default:
      return false;
  }
}

std::string PartitionerFactory::getDescription(PartitionStrategy strategy) {
  switch (strategy) {
    case PartitionStrategy::ROUND_ROBIN:
      return "Round-robin partitioner: distributes records evenly across "
             "partitions in order. Best with SharedWindowState.";

    case PartitionStrategy::KEY_HASH:
      return "Key-based hash partitioner: uses timestamp for consistent "
             "hashing. Ensures temporal locality.";

    case PartitionStrategy::VECTOR_HASH:
      return "Vector hash partitioner: uses first dimensions for hashing. "
             "Simple but limited locality preservation.";

    case PartitionStrategy::LSH:
      return "LSH partitioner: uses locality-sensitive hashing for vector "
             "space partitioning. High probability of same partition for "
             "similar vectors. Used in VSJoin.";

    case PartitionStrategy::CENTROID:
      return "Centroid-based partitioner: uses K-means clustering for "
             "vector space partitioning. Requires training. Used in "
             "S3J/ClusteredJoin.";

    default:
      return "Unknown partitioner strategy.";
  }
}

}  // namespace sageFlow
