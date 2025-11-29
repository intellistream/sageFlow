# Task B-04: AsyncCandidateGenerator 异步候选生成器

**优先级**: 🟡 中  
**预估工时**: 2-3 天  
**依赖**: A-05 (DistanceVerifier) ✅, B-01 (PartitionedIndex)  
**输出文件**:
- `include/operator/async_candidate_generator.h`
- `src/operator/async_candidate_generator.cpp`
- `test/UnitTest/test_async_candidate_generator.cpp`

---

## 任务描述

实现异步候选生成器，解耦候选生成和距离验证，实现流水线化处理。

---

## 提示词

```
你是 sageFlow 项目的开发者，需要实现 AsyncCandidateGenerator 类。

## 项目背景
sageFlow 是一个 C++20 流式向量处理引擎，遵循以下规范：
- 类名: CamelCase (如 AsyncCandidateGenerator)
- 方法名: camelBack (如 submitQuery, submitBatch)
- 成员变量: lower_case_ 带尾部下划线 (如 task_queue_, workers_)
- 使用 #pragma once 作为头文件保护
- 使用 spdlog 进行日志记录 (SAGEFLOW_LOG_* 宏)

## 背景
当前候选生成是同步的，阻塞处理流程。
异步候选生成可以实现：
1. 候选生成与验证的流水线化
2. 批量查询优化
3. 提高 CPU 利用率

## 依赖
- DistanceVerifier (A-05): 已实现于 include/operator/distance_verifier.h
- PartitionedIndex (B-01): 需要先完成或使用现有 Index 接口

注意：由于 B-01 可能尚未完成，此任务可以先使用 Index 基类接口实现，
待 B-01 完成后再切换为 PartitionedIndex。

## 文件位置
- 头文件: include/operator/async_candidate_generator.h
- 实现文件: src/operator/async_candidate_generator.cpp

## 接口要求

```cpp
#pragma once

#include "common/vector_record.h"
#include "index/index.h"
#include "operator/distance_verifier.h"
#include <vector>
#include <memory>
#include <future>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>

namespace sageFlow {

/**
 * @brief 候选查询请求
 */
struct CandidateQuery {
    const VectorRecord* query;      ///< 查询向量
    int k;                          ///< 返回数量
    size_t request_id;              ///< 请求ID
};

/**
 * @brief 候选查询结果
 */
struct CandidateResult {
    size_t request_id;                                    ///< 请求ID
    std::vector<std::unique_ptr<VectorRecord>> candidates; ///< 候选向量
    bool success;                                          ///< 是否成功
    std::string error_msg;                                 ///< 错误信息
};

/**
 * @brief 异步候选生成器
 * 
 * 使用线程池异步执行索引查询，支持批量查询和流水线处理。
 */
class AsyncCandidateGenerator {
public:
    /**
     * @brief 构造函数
     * @param index 索引（可以是 Index 基类或 PartitionedIndex）
     * @param num_threads 工作线程数
     * @param max_queue_size 最大队列大小（0=无限制）
     */
    explicit AsyncCandidateGenerator(
        std::shared_ptr<Index> index,
        size_t num_threads = 4,
        size_t max_queue_size = 1000);
    
    /**
     * @brief 析构函数 - 自动关闭
     */
    ~AsyncCandidateGenerator();
    
    // 禁用拷贝
    AsyncCandidateGenerator(const AsyncCandidateGenerator&) = delete;
    AsyncCandidateGenerator& operator=(const AsyncCandidateGenerator&) = delete;
    
    /**
     * @brief 提交查询请求
     * @param query 查询向量
     * @param k 返回数量
     * @return 异步结果的 future
     */
    std::future<std::vector<std::unique_ptr<VectorRecord>>> 
        submitQuery(const VectorRecord& query, int k);
    
    /**
     * @brief 批量提交查询
     * @param queries 查询向量列表
     * @param k 每个查询的返回数量
     * @return 异步结果的 future 列表
     */
    std::vector<std::future<std::vector<std::unique_ptr<VectorRecord>>>>
        submitBatch(const std::vector<const VectorRecord*>& queries, int k);
    
    /**
     * @brief 提交查询并验证
     * @param query 查询向量
     * @param k 返回数量
     * @param verifier 距离验证器
     * @return 验证通过的候选
     */
    std::future<std::vector<std::unique_ptr<VectorRecord>>>
        submitQueryWithVerification(const VectorRecord& query, int k,
                                    std::shared_ptr<DistanceVerifier> verifier);
    
    /**
     * @brief 获取待处理查询数量
     */
    size_t getPendingCount() const;
    
    /**
     * @brief 获取已完成查询数量
     */
    uint64_t getCompletedCount() const { return completed_count_.load(); }
    
    /**
     * @brief 关闭生成器（等待所有任务完成）
     */
    void shutdown();
    
    /**
     * @brief 强制关闭（丢弃未完成任务）
     */
    void shutdownNow();
    
    /**
     * @brief 是否正在运行
     */
    bool isRunning() const { return running_.load(); }
    
    /**
     * @brief 获取工作线程数
     */
    size_t getNumThreads() const { return num_threads_; }

private:
    std::shared_ptr<Index> index_;
    size_t num_threads_;
    size_t max_queue_size_;
    
    // 任务定义
    struct Task {
        CandidateQuery query;
        std::promise<std::vector<std::unique_ptr<VectorRecord>>> promise;
        std::shared_ptr<DistanceVerifier> verifier;  // 可选的验证器
    };
    
    // 任务队列
    std::queue<std::unique_ptr<Task>> task_queue_;
    mutable std::mutex queue_mutex_;
    std::condition_variable queue_not_empty_;
    std::condition_variable queue_not_full_;
    
    // 工作线程
    std::vector<std::thread> workers_;
    std::atomic<bool> running_{true};
    std::atomic<bool> shutdown_requested_{false};
    
    // 统计
    std::atomic<uint64_t> completed_count_{0};
    std::atomic<size_t> request_id_counter_{0};
    
    /**
     * @brief 工作线程循环
     */
    void workerLoop();
    
    /**
     * @brief 执行单个查询
     */
    std::vector<std::unique_ptr<VectorRecord>> executeQuery(
        const CandidateQuery& query,
        std::shared_ptr<DistanceVerifier> verifier);
    
    /**
     * @brief 生成请求ID
     */
    size_t generateRequestId() { return request_id_counter_++; }
};

} // namespace sageFlow
```

## 实现要点

1. **构造函数**:
   - 启动 num_threads 个工作线程
   - 每个线程运行 workerLoop()
   - 设置 running_ = true

2. **submitQuery()**:
   ```cpp
   std::future<std::vector<std::unique_ptr<VectorRecord>>> 
   submitQuery(const VectorRecord& query, int k) {
       auto task = std::make_unique<Task>();
       task->query.query = &query;
       task->query.k = k;
       task->query.request_id = generateRequestId();
       task->verifier = nullptr;
       
       auto future = task->promise.get_future();
       
       {
           std::unique_lock<std::mutex> lock(queue_mutex_);
           
           // 如果设置了最大队列大小，等待队列有空间
           if (max_queue_size_ > 0) {
               queue_not_full_.wait(lock, [this] {
                   return task_queue_.size() < max_queue_size_ || !running_;
               });
           }
           
           if (!running_) {
               task->promise.set_exception(
                   std::make_exception_ptr(std::runtime_error("Generator shutdown")));
               return future;
           }
           
           task_queue_.push(std::move(task));
       }
       queue_not_empty_.notify_one();
       
       return future;
   }
   ```

3. **workerLoop()**:
   ```cpp
   void workerLoop() {
       while (running_ || !task_queue_.empty()) {
           std::unique_ptr<Task> task;
           {
               std::unique_lock<std::mutex> lock(queue_mutex_);
               queue_not_empty_.wait(lock, [this] { 
                   return !task_queue_.empty() || !running_; 
               });
               
               if (!running_ && task_queue_.empty()) break;
               if (task_queue_.empty()) continue;
               
               task = std::move(task_queue_.front());
               task_queue_.pop();
           }
           queue_not_full_.notify_one();
           
           try {
               auto result = executeQuery(task->query, task->verifier);
               task->promise.set_value(std::move(result));
           } catch (...) {
               task->promise.set_exception(std::current_exception());
           }
           
           completed_count_++;
       }
   }
   ```

4. **executeQuery()**:
   ```cpp
   std::vector<std::unique_ptr<VectorRecord>> executeQuery(
       const CandidateQuery& query,
       std::shared_ptr<DistanceVerifier> verifier) {
       
       // 执行索引查询
       auto results = index_->query(*query.query, query.k);
       
       // 转换为 unique_ptr
       std::vector<std::unique_ptr<VectorRecord>> candidates;
       for (const auto& r : results) {
           candidates.push_back(std::make_unique<VectorRecord>(*r));
       }
       
       // 如果有验证器，进行验证过滤
       if (verifier) {
           candidates = verifier->filterCandidates(*query.query, 
                                                    std::move(candidates));
       }
       
       return candidates;
   }
   ```

5. **shutdown()**:
   - 设置 shutdown_requested_ = true
   - 等待队列清空
   - 设置 running_ = false
   - 通知所有工作线程
   - join 所有线程

## 测试要求

```cpp
#include <gtest/gtest.h>
#include "operator/async_candidate_generator.h"
#include "index/bruteforce.h"

class AsyncCandidateGeneratorTest : public ::testing::Test {
protected:
    void SetUp() override {
        index_ = std::make_shared<Bruteforce>(128);
        // 插入测试数据
        for (int i = 0; i < 100; ++i) {
            index_->insert(createRandomRecord(i));
        }
        generator_ = std::make_unique<AsyncCandidateGenerator>(index_, 4);
    }
    
    void TearDown() override {
        if (generator_) {
            generator_->shutdown();
        }
    }
    
    std::shared_ptr<Index> index_;
    std::unique_ptr<AsyncCandidateGenerator> generator_;
    
    std::unique_ptr<VectorRecord> createRandomRecord(uint64_t uid);
};

// 基础功能测试
TEST_F(AsyncCandidateGeneratorTest, SingleQuery) {
    // 测试单个异步查询
}

TEST_F(AsyncCandidateGeneratorTest, SingleQueryResult) {
    // 测试查询结果正确性
}

TEST_F(AsyncCandidateGeneratorTest, BatchQuery) {
    // 测试批量查询
}

TEST_F(AsyncCandidateGeneratorTest, BatchQueryOrder) {
    // 测试批量查询结果顺序
}

// 验证器集成测试
TEST_F(AsyncCandidateGeneratorTest, QueryWithVerification) {
    // 测试带验证的查询
}

// 并发测试
TEST_F(AsyncCandidateGeneratorTest, ConcurrentSubmit) {
    // 测试并发提交
}

TEST_F(AsyncCandidateGeneratorTest, HighConcurrency) {
    // 测试高并发场景
}

// 生命周期测试
TEST_F(AsyncCandidateGeneratorTest, GracefulShutdown) {
    // 测试优雅关闭
}

TEST_F(AsyncCandidateGeneratorTest, ShutdownNow) {
    // 测试强制关闭
}

TEST_F(AsyncCandidateGeneratorTest, ShutdownWithPendingTasks) {
    // 测试有待处理任务时关闭
}

// 统计测试
TEST_F(AsyncCandidateGeneratorTest, PendingCount) {
    // 测试待处理计数
}

TEST_F(AsyncCandidateGeneratorTest, CompletedCount) {
    // 测试完成计数
}

// 边界条件测试
TEST_F(AsyncCandidateGeneratorTest, EmptyIndex) {
    // 测试空索引
}

TEST_F(AsyncCandidateGeneratorTest, ZeroK) {
    // 测试 k=0
}
```

## 验收标准
1. 所有单元测试通过
2. 异步结果正确
3. 无内存泄漏（使用 valgrind 或 AddressSanitizer）
4. 无死锁
5. 代码通过 clang-tidy 检查
```
