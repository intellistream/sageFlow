#pragma once

#include "operator/join_strategy_config.h"
#include "operator/join_operator_methods/base_method.h"
#include "concurrency/concurrency_manager.h"

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace sageFlow {

/**
 * @brief Join 方法注册中心
 *
 * 单例模式，用于管理所有 Baseline 方法的注册和创建。
 * 每个 Join 方法在其 .cpp 文件末尾通过 REGISTER_JOIN_METHOD 宏自动注册。
 *
 * 使用示例：
 * @code
 * // 获取所有已注册的方法
 * auto methods = JoinMethodRegistry::instance().getAvailableMethods();
 *
 * // 创建特定算法的方法实例
 * auto method = JoinMethodRegistry::instance().createMethod(
 *     JoinAlgorithm::BRUTEFORCE, config, cm, dimension, left_idx, right_idx);
 * @endcode
 */
class JoinMethodRegistry {
 public:
    /**
     * @brief 方法创建器类型
     *
     * @param config 策略配置
     * @param concurrency_manager 并发管理器
     * @param dimension 向量维度
     * @param left_index_id 左流索引 ID
     * @param right_index_id 右流索引 ID
     * @return 创建的方法实例
     */
    using MethodCreator = std::function<std::unique_ptr<BaseMethod>(
        const JoinStrategyConfig& config,
        std::shared_ptr<ConcurrencyManager> concurrency_manager,
        int dimension,
        int left_index_id,
        int right_index_id)>;

    /**
     * @brief 方法元信息
     *
     * 描述一个 Join 方法的特性和推荐配置。
     */
    struct MethodInfo {
        std::string name;                          ///< 方法名称（如 "BruteForce"）
        std::string description;                   ///< 方法描述
        JoinAlgorithm algorithm;                   ///< 算法类型
        bool supports_eager;                       ///< 是否支持 Eager 模式
        bool supports_lazy;                        ///< 是否支持 Lazy 模式
        PartitionStrategy recommended_partition;   ///< 推荐的分区策略
        WindowStateType recommended_window_state;  ///< 推荐的窗口状态类型
        std::string paper_reference;               ///< 论文引用（可选）
    };

    /**
     * @brief 获取单例实例
     * @return 注册中心单例引用
     */
    static JoinMethodRegistry& instance();

    /**
     * @brief 注册方法
     *
     * @param algorithm 算法类型
     * @param info 方法元信息
     * @param creator 创建器函数
     */
    void registerMethod(JoinAlgorithm algorithm, MethodInfo info, MethodCreator creator);

    /**
     * @brief 创建方法实例
     *
     * @param algorithm 算法类型
     * @param config 策略配置
     * @param concurrency_manager 并发管理器
     * @param dimension 向量维度
     * @param left_index_id 左流索引 ID（-1 表示无索引）
     * @param right_index_id 右流索引 ID（-1 表示无索引）
     * @return 方法实例
     * @throws std::runtime_error 如果算法未注册
     */
    std::unique_ptr<BaseMethod> createMethod(JoinAlgorithm algorithm,
                                             const JoinStrategyConfig& config,
                                             std::shared_ptr<ConcurrencyManager> concurrency_manager,
                                             int dimension,
                                             int left_index_id = -1,
                                             int right_index_id = -1);

    /**
     * @brief 获取所有可用方法的元信息
     * @return 方法元信息列表
     */
    [[nodiscard]] std::vector<MethodInfo> getAvailableMethods() const;

    /**
     * @brief 获取指定方法的元信息
     * @param algorithm 算法类型
     * @return 方法元信息
     * @throws std::runtime_error 如果算法未注册
     */
    [[nodiscard]] const MethodInfo& getMethodInfo(JoinAlgorithm algorithm) const;

    /**
     * @brief 检查方法是否已注册
     * @param algorithm 算法类型
     * @return true 如果已注册
     */
    [[nodiscard]] bool hasMethod(JoinAlgorithm algorithm) const;

    /**
     * @brief 获取已注册方法数量
     * @return 已注册的方法数量
     */
    [[nodiscard]] size_t getRegisteredCount() const;

    /**
     * @brief 获取算法的推荐配置
     *
     * 根据算法的推荐配置填充 JoinStrategyConfig
     *
     * @param algorithm 算法类型
     * @param config 要填充的配置
     * @return true 如果成功
     */
    bool applyRecommendedConfig(JoinAlgorithm algorithm, JoinStrategyConfig& config) const;

 private:
    JoinMethodRegistry() = default;
    JoinMethodRegistry(const JoinMethodRegistry&) = delete;
    JoinMethodRegistry& operator=(const JoinMethodRegistry&) = delete;

    std::unordered_map<JoinAlgorithm, MethodInfo> infos_;
    std::unordered_map<JoinAlgorithm, MethodCreator> creators_;
    mutable std::mutex mutex_;
};

/**
 * @brief 自动注册宏
 *
 * 在各 Baseline 的 .cpp 文件末尾调用此宏，实现编译时自动注册。
 *
 * 使用示例：
 * @code
 * REGISTER_JOIN_METHOD(
 *     sageFlow::JoinAlgorithm::BRUTEFORCE,
 *     (sageFlow::JoinMethodRegistry::MethodInfo{
 *         "BruteForce",
 *         "Ground truth baseline",
 *         sageFlow::JoinAlgorithm::BRUTEFORCE,
 *         true,
 *         true,
 *         sageFlow::PartitionStrategy::ROUND_ROBIN,
 *         sageFlow::WindowStateType::SHARED,
 *         ""
 *     }),
 *     [](const auto& config, auto cm, int dim, int left_idx, int right_idx) {
 *         return std::make_unique<sageFlow::BruteForceMethod>(...);
 *     }
 * );
 * @endcode
 *
 * @note Info 参数必须用括号包裹以避免逗号被解析为宏参数分隔符
 *
 * @param Algorithm JoinAlgorithm 枚举值
 * @param Info MethodInfo 结构体（用括号包裹）
 * @param Creator 创建器 lambda 或函数
 */
#define SAGEFLOW_CONCAT_IMPL(a, b) a##b
#define SAGEFLOW_CONCAT(a, b) SAGEFLOW_CONCAT_IMPL(a, b)

#define REGISTER_JOIN_METHOD(Algorithm, Info, Creator)                             \
    namespace {                                                                    \
    static bool SAGEFLOW_CONCAT(_registered_, __LINE__) = []() {                   \
        ::sageFlow::JoinMethodRegistry::instance().registerMethod(Algorithm, Info, \
                                                                  Creator);        \
        return true;                                                               \
    }();                                                                           \
    }

}  // namespace sageFlow
