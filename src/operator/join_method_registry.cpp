#include "operator/join_method_registry.h"
#include "utils/logger.h"

#include <sstream>
#include <stdexcept>

namespace sageFlow {

JoinMethodRegistry& JoinMethodRegistry::instance() {
    static JoinMethodRegistry instance;
    return instance;
}

void JoinMethodRegistry::registerMethod(JoinAlgorithm algorithm, MethodInfo info, MethodCreator creator) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (creators_.find(algorithm) != creators_.end()) {
        SAGEFLOW_LOG_WARN("JoinMethodRegistry", "Method {} already registered, overwriting", info.name);
    }

    infos_[algorithm] = std::move(info);
    creators_[algorithm] = std::move(creator);

    SAGEFLOW_LOG_DEBUG("JoinMethodRegistry", "Registered join method: {}", infos_[algorithm].name);
}

std::unique_ptr<BaseMethod> JoinMethodRegistry::createMethod(JoinAlgorithm algorithm,
                                                             const JoinStrategyConfig& config,
                                                             std::shared_ptr<ConcurrencyManager> concurrency_manager,
                                                             int dimension,
                                                             int left_index_id,
                                                             int right_index_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = creators_.find(algorithm);
    if (it == creators_.end()) {
        throw std::runtime_error("Unknown join algorithm: " + toString(algorithm));
    }

    SAGEFLOW_LOG_DEBUG("JoinMethodRegistry", "Creating method for algorithm: {}", toString(algorithm));
    return it->second(config, concurrency_manager, dimension, left_index_id, right_index_id);
}

std::vector<JoinMethodRegistry::MethodInfo> JoinMethodRegistry::getAvailableMethods() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<MethodInfo> result;
    result.reserve(infos_.size());

    for (const auto& [algo, info] : infos_) {
        result.push_back(info);
    }

    return result;
}

const JoinMethodRegistry::MethodInfo& JoinMethodRegistry::getMethodInfo(JoinAlgorithm algorithm) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = infos_.find(algorithm);
    if (it == infos_.end()) {
        throw std::runtime_error("Method info not found for: " + toString(algorithm));
    }

    return it->second;
}

bool JoinMethodRegistry::hasMethod(JoinAlgorithm algorithm) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return creators_.find(algorithm) != creators_.end();
}

size_t JoinMethodRegistry::getRegisteredCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return creators_.size();
}

bool JoinMethodRegistry::applyRecommendedConfig(JoinAlgorithm algorithm, JoinStrategyConfig& config) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = infos_.find(algorithm);
    if (it == infos_.end()) {
        return false;
    }

    const auto& info = it->second;
    config.algorithm = info.algorithm;
    config.partition_strategy = info.recommended_partition;
    config.window_state_type = info.recommended_window_state;

    return true;
}

}  // namespace sageFlow
