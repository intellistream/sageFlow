//
// Created for sageFlow architecture refactoring - Phase 2
// Task C-05: JoinMethodRegistry 方法注册系统
//

#include "operator/utils/join_method_registry.h"
#include "utils/logger.h"

#include <stdexcept>

namespace sageFlow {

// ==================== 单例实现 ====================

JoinMethodRegistry& JoinMethodRegistry::instance() {
    static JoinMethodRegistry instance;
    return instance;
}

// ==================== 注册方法实现 ====================

void JoinMethodRegistry::registerMethod(
    JoinAlgorithm algorithm,
    MethodInfo info,
    MethodCreator creator) {
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (infos_.find(algorithm) != infos_.end()) {
        SAGEFLOW_LOG_WARN("JoinMethodRegistry", 
            "Method {} already registered, overwriting", info.name);
    }
    
    infos_[algorithm] = std::move(info);
    creators_[algorithm] = std::move(creator);
    SAGEFLOW_LOG_DEBUG("JoinMethodRegistry", 
        "Registered method: {} (algo={})", infos_[algorithm].name, static_cast<int>(algorithm));
}

// ==================== 查询方法实现 ====================

std::unique_ptr<BaseMethod> JoinMethodRegistry::createMethod(
    JoinAlgorithm algorithm,
    const JoinStrategyConfig& config,
    std::shared_ptr<ConcurrencyManager> cm,
    int dimension,
    int left_index_id,
    int right_index_id) {
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = creators_.find(algorithm);
    if (it == creators_.end()) {
        throw std::runtime_error("Method not found for algorithm: " + std::to_string(static_cast<int>(algorithm)));
    }
    
    return it->second(config, std::move(cm), dimension, left_index_id, right_index_id);
}

std::vector<JoinMethodRegistry::MethodInfo> JoinMethodRegistry::getAvailableMethods() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::vector<MethodInfo> methods;
    methods.reserve(infos_.size());
    
    for (const auto& [algo, info] : infos_) {
        methods.push_back(info);
    }
    
    return methods;
}

const JoinMethodRegistry::MethodInfo& JoinMethodRegistry::getMethodInfo(
    JoinAlgorithm algorithm) const {
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = infos_.find(algorithm);
    if (it == infos_.end()) {
        throw std::runtime_error("Method not found for algorithm: " + std::to_string(static_cast<int>(algorithm)));
    }
    
    return it->second;
}

bool JoinMethodRegistry::hasMethod(JoinAlgorithm algorithm) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return infos_.find(algorithm) != infos_.end();
}

size_t JoinMethodRegistry::getRegisteredCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return infos_.size();
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
