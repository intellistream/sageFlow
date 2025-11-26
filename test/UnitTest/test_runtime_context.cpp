#include <gtest/gtest.h>
#include "execution/runtime_context.h"

namespace sageFlow {
namespace test {

/**
 * @brief 测试 RuntimeContext 的基本功能
 * 验证运行时上下文能够正确提供线程身份信息
 */
class RuntimeContextTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 测试基础设施
    }

    void TearDown() override {
        // 清理
    }
};

TEST_F(RuntimeContextTest, BasicContextCreation) {
    // 创建不同的运行时上下文
    RuntimeContext ctx1(0, 1);  // 单实例
    RuntimeContext ctx2(3, 8);  // 8实例中的第4个（索引3）
    
    // 验证子任务索引
    EXPECT_EQ(ctx1.getSubtaskIndex(), 0);
    EXPECT_EQ(ctx2.getSubtaskIndex(), 3);
    
    // 验证并行度
    EXPECT_EQ(ctx1.getParallelism(), 1);
    EXPECT_EQ(ctx2.getParallelism(), 8);
}

TEST_F(RuntimeContextTest, TaskNameFormatting) {
    RuntimeContext ctx1(0, 1);
    RuntimeContext ctx2(3, 8);
    RuntimeContext ctx3(15, 16);
    
    // 验证任务名称格式
    EXPECT_EQ(ctx1.getTaskName(), "Task[0/1]");
    EXPECT_EQ(ctx2.getTaskName(), "Task[3/8]");
    EXPECT_EQ(ctx3.getTaskName(), "Task[15/16]");
}

TEST_F(RuntimeContextTest, ContextCopy) {
    RuntimeContext ctx1(5, 10);
    
    // 测试拷贝构造
    RuntimeContext ctx2 = ctx1;
    
    EXPECT_EQ(ctx2.getSubtaskIndex(), 5);
    EXPECT_EQ(ctx2.getParallelism(), 10);
    EXPECT_EQ(ctx2.getTaskName(), ctx1.getTaskName());
}

TEST_F(RuntimeContextTest, DifferentParallelisms) {
    // 测试各种并行度场景
    std::vector<size_t> parallelisms = {1, 2, 4, 8, 16, 32, 64};
    
    for (size_t p : parallelisms) {
        for (size_t i = 0; i < p; ++i) {
            RuntimeContext ctx(i, p);
            
            EXPECT_EQ(ctx.getSubtaskIndex(), i);
            EXPECT_EQ(ctx.getParallelism(), p);
            EXPECT_LT(ctx.getSubtaskIndex(), ctx.getParallelism());
        }
    }
}

} // namespace test
} // namespace sageFlow
