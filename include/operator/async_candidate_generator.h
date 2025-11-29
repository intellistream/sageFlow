#pragma once

#include "common/data_types.h"
#include "index/index.h"
#include "operator/distance_verifier.h"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace sageFlow {

/**
 * @brief 候选查询请求
 */
struct CandidateQuery {
  std::unique_ptr<VectorRecord> query;  ///< 查询向量（拥有所有权）
  int k;                                ///< 返回数量
  size_t request_id;                    ///< 请求ID

  CandidateQuery() : k(0), request_id(0) {}
  CandidateQuery(std::unique_ptr<VectorRecord> q, int k_val, size_t id)
      : query(std::move(q)), k(k_val), request_id(id) {}
};

/**
 * @brief 候选查询结果
 */
struct CandidateResult {
  size_t request_id;                                      ///< 请求ID
  std::vector<std::unique_ptr<VectorRecord>> candidates;  ///< 候选向量
  bool success;                                           ///< 是否成功
  std::string error_msg;                                  ///< 错误信息

  CandidateResult() : request_id(0), success(false) {}

  CandidateResult(size_t id, std::vector<std::unique_ptr<VectorRecord>>&& cands, bool ok,
                  std::string msg = "")
      : request_id(id), candidates(std::move(cands)), success(ok), error_msg(std::move(msg)) {}
};

/**
 * @brief 异步候选生成器
 *
 * 使用线程池异步执行索引查询，支持批量查询和流水线处理。
 * 解耦候选生成和距离验证，实现高效的流水线化处理。
 */
class AsyncCandidateGenerator {
 public:
  /**
   * @brief 构造函数
   * @param index 索引（可以是 Index 基类或 PartitionedIndex）
   * @param num_threads 工作线程数
   * @param max_queue_size 最大队列大小（0=无限制）
   */
  explicit AsyncCandidateGenerator(std::shared_ptr<Index> index, size_t num_threads = 4,
                                   size_t max_queue_size = 1000);

  /**
   * @brief 析构函数 - 自动关闭
   */
  ~AsyncCandidateGenerator();

  // 禁用拷贝
  AsyncCandidateGenerator(const AsyncCandidateGenerator&) = delete;
  AsyncCandidateGenerator& operator=(const AsyncCandidateGenerator&) = delete;

  // 禁用移动（由于线程管理复杂性）
  AsyncCandidateGenerator(AsyncCandidateGenerator&&) = delete;
  AsyncCandidateGenerator& operator=(AsyncCandidateGenerator&&) = delete;

  /**
   * @brief 提交查询请求
   * @param query 查询向量
   * @param k 返回数量
   * @return 异步结果的 future
   */
  std::future<std::vector<std::unique_ptr<VectorRecord>>> submitQuery(const VectorRecord& query,
                                                                       int k);

  /**
   * @brief 批量提交查询
   * @param queries 查询向量列表
   * @param k 每个查询的返回数量
   * @return 异步结果的 future 列表
   */
  std::vector<std::future<std::vector<std::unique_ptr<VectorRecord>>>> submitBatch(
      const std::vector<const VectorRecord*>& queries, int k);

  /**
   * @brief 提交查询并验证
   * @param query 查询向量
   * @param k 返回数量
   * @param verifier 距离验证器
   * @return 验证通过的候选
   */
  std::future<std::vector<std::unique_ptr<VectorRecord>>> submitQueryWithVerification(
      const VectorRecord& query, int k, std::shared_ptr<DistanceVerifier> verifier);

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
   * @brief 是否已请求关闭
   */
  bool isShutdownRequested() const { return shutdown_requested_.load(); }

  /**
   * @brief 获取工作线程数
   */
  size_t getNumThreads() const { return num_threads_; }

  /**
   * @brief 获取最大队列大小
   */
  size_t getMaxQueueSize() const { return max_queue_size_; }

 private:
  std::shared_ptr<Index> index_;
  size_t num_threads_;
  size_t max_queue_size_;

  // 任务定义
  struct Task {
    CandidateQuery query;
    std::promise<std::vector<std::unique_ptr<VectorRecord>>> promise;
    std::shared_ptr<DistanceVerifier> verifier;  // 可选的验证器

    Task() : verifier(nullptr) {}
    Task(CandidateQuery&& q, std::shared_ptr<DistanceVerifier> v = nullptr)
        : query(std::move(q)), verifier(std::move(v)) {}
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
   * @param query 查询请求
   * @param verifier 可选的距离验证器
   * @return 候选向量列表
   */
  std::vector<std::unique_ptr<VectorRecord>> executeQuery(
      const CandidateQuery& query, std::shared_ptr<DistanceVerifier> verifier);

  /**
   * @brief 生成请求ID
   */
  size_t generateRequestId() { return request_id_counter_.fetch_add(1); }
};

}  // namespace sageFlow
