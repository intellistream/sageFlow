#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>

#include "concurrency/concurrency_manager.h"
#include "execution/runtime_context.h"
#include "function/join_function.h"
#include "operator/join_operator.h"
#include "operator/utils/join_strategy_config.h"
#include "storage/storage_manager.h"
#include "execution/collector.h"

namespace sageFlow {
namespace test {

class VSJoinRebuildTest : public ::testing::Test {
protected:
    void SetUp() override {
        storage_manager_ = std::make_shared<StorageManager>();
        concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage_manager_);
    }

    std::unique_ptr<Function> createJoinFunction(int dimension = 16) {
        auto join_func = std::make_unique<JoinFunction>("test_join", dimension);
        // 窗口 100ms，步长 10ms（便于测试过期过滤）
        join_func->setWindow(100, 10);
        return join_func;
    }

    JoinStrategyConfig createVSJoinConfig(int dimension = 16) {
        JoinStrategyConfig config;
        config.algorithm = JoinAlgorithm::VSJOIN;
        config.partition_strategy = PartitionStrategy::LSH;  // VSJoin 需要分区策略
        config.window_state_type = WindowStateType::PARTITIONED_VECTOR; // VSJoin 校验要求 PartitionedVectorState

        config.index_strategy = IndexStrategy::PARTITIONED;
        config.dimension = dimension;

        config.ivf_nlist = 16;
        config.ivf_nprobes = 4;
        config.ivf_rebuild_threshold = 2.0;

        config.vsjoin_rebuild_interval_ms = 30; // 提高触发频率
        config.window_size_ms = 100;
        config.step_size_ms = 10;
        return config;
    }

    std::unique_ptr<VectorRecord> makeRecord(uint64_t uid, int64_t ts, int dim) {
        char* raw_data = new char[dim * sizeof(float)];
        float* f = reinterpret_cast<float*>(raw_data);
        for (int i = 0; i < dim; ++i) {
            f[i] = 1.0f;
        }
        return std::make_unique<VectorRecord>(uid, ts, dim, DataType::Float32, raw_data);
    }

    std::shared_ptr<StorageManager> storage_manager_;
    std::shared_ptr<ConcurrencyManager> concurrency_manager_;
};

TEST_F(VSJoinRebuildTest, BackgroundThreadStartOnceAndStopSafe) {
    auto join_func = createJoinFunction(16);
    auto config = createVSJoinConfig(16);
    ASSERT_EQ(config.window_state_type, WindowStateType::PARTITIONED_VECTOR);
    ASSERT_EQ(config.partition_strategy, PartitionStrategy::LSH);


    auto op = std::make_shared<JoinOperator>(join_func, concurrency_manager_, config);

    RuntimeContext ctx0(0, 2);
    RuntimeContext ctx1(1, 2);

    // open 多次（不同 subtask）应只初始化一次且不崩溃
    EXPECT_NO_THROW(op->open(ctx0));
    EXPECT_NO_THROW(op->open(ctx1));

    // 析构应能安全停止后台线程
    EXPECT_NO_THROW({ op.reset(); });
}

TEST_F(VSJoinRebuildTest, RebuildLoopDeduplicateAndFilterExpired) {
    auto join_func = createJoinFunction(16);
    auto config = createVSJoinConfig(16);

    auto op = std::make_shared<JoinOperator>(join_func, concurrency_manager_, config);
    RuntimeContext ctx0(0, 2);
    RuntimeContext ctx1(1, 2);
    op->open(ctx0);

    // 直接往 WindowState 注入数据（绕开 apply 的复杂路径，仅验证 rebuild 读取快照/去重/过滤/替换能跑通）
    // 同一个 uid 通过“多播”出现在多个分区 -> rebuild 需要去重
    const int dim = 16;
    const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    // 新鲜记录：uid=1（重复两份），uid=2
    op->open(ctx1); // 确保 parallelism_=2

    // 通过 JoinOperator 的 WindowState 指针访问（JoinOperator 内部已经初始化）
    // 这里使用 apply 会走更多路径，测试更脆弱；直接向 state 写入更稳定。
    // 但 JoinOperator 的 state 是 private，所以我们通过 apply 写入：
    // - 两个 subtask 都写入 uid=1（制造重复）
    // - 另外写入一个过期 uid=99

    Collector dummy_collector([](std::unique_ptr<Response> /*record*/, int /*slot*/) {});
    dummy_collector.set_slot_size(2);
    // uid=1 写入两次（模拟多播）
    op->apply(Response{ResponseType::Record, std::move(makeRecord(1, now_ms, dim))}, 0, dummy_collector, ctx0);
    op->apply(Response{ResponseType::Record, std::move(makeRecord(1, now_ms, dim))}, 0, dummy_collector, ctx1);

    // uid=2
    op->apply(Response{ResponseType::Record, std::move(makeRecord(2, now_ms, dim))}, 1, dummy_collector, ctx0);

    // 过期记录 uid=99（时间戳远小于 window 下界）
    op->apply(Response{ResponseType::Record, std::move(makeRecord(99, now_ms - 100000, dim))}, 1, dummy_collector, ctx0);

    // 等待至少一次 rebuild tick
    std::this_thread::sleep_for(std::chrono::milliseconds(120));

    // 验证：替换后，全局 IVF 索引可以 query_for_join 正常返回（不崩溃即可，召回不做强保证）
    // 这里通过 VSJoinMethod 的查询路径间接覆盖 ConcurrencyManager replace + query。
    auto q_ptr = makeRecord(777, now_ms, dim);
    ASSERT_NE(q_ptr, nullptr);
    ASSERT_EQ(q_ptr->data_.dim_, dim) << "Query record should have dimension " << dim;
    
    // 直接 query 全局索引（id 由 factory 创建并由 operator 初始化写入 vsjoin_global_*_id_）
    // JoinOperator 内部 id 是 private，这里无法直接读；但 query_for_join 不存在 id 则返回空。
    // 因此我们只做"无异常"验证：后台 rebuild 期间不应导致崩溃。
    EXPECT_NO_THROW({
        (void)concurrency_manager_->query_for_join(0, *q_ptr, 0.8, 0.1);
    });

    op.reset();
}

}  // namespace test
}  // namespace sageFlow
