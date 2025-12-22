#include "operator/join_operator_methods/vsjoin_components/async_candidate_generator.h"

#include <stdexcept>

#include "utils/logger.h"

namespace sageFlow {

AsyncCandidateGenerator::AsyncCandidateGenerator(std::shared_ptr<Index> index, size_t num_threads,
                                                 size_t max_queue_size)
    : index_(std::move(index)), num_threads_(num_threads), max_queue_size_(max_queue_size) {
  if (index_ == nullptr) {
    throw std::invalid_argument("Index cannot be null");
  }
  if (num_threads_ == 0) {
    throw std::invalid_argument("Number of threads must be greater than 0");
  }

  SAGEFLOW_LOG_INFO("AsyncCandGen", "Starting AsyncCandidateGenerator with {} threads, max_queue={}",
                    num_threads_, max_queue_size_);

  // 启动工作线程
  workers_.reserve(num_threads_);
  for (size_t i = 0; i < num_threads_; ++i) {
    workers_.emplace_back(&AsyncCandidateGenerator::workerLoop, this);
  }
}

AsyncCandidateGenerator::~AsyncCandidateGenerator() {
  if (running_.load()) {
    shutdown();
  }
}

std::future<std::vector<std::unique_ptr<VectorRecord>>> AsyncCandidateGenerator::submitQuery(
    const VectorRecord& query, int k) {
  // 复制查询向量以确保生命周期安全
  auto query_copy = std::make_unique<VectorRecord>(query);
  CandidateQuery cq(std::move(query_copy), k, generateRequestId());
  auto task = std::make_unique<Task>(std::move(cq), nullptr);

  auto future = task->promise.get_future();

  {
    std::unique_lock<std::mutex> lock(queue_mutex_);

    // 如果设置了最大队列大小，等待队列有空间
    if (max_queue_size_ > 0) {
      queue_not_full_.wait(lock, [this] {
        return task_queue_.size() < max_queue_size_ || !running_.load();
      });
    }

    if (!running_.load()) {
      task->promise.set_exception(
          std::make_exception_ptr(std::runtime_error("AsyncCandidateGenerator is shutdown")));
      return future;
    }

    task_queue_.push(std::move(task));
  }
  queue_not_empty_.notify_one();

  return future;
}

std::vector<std::future<std::vector<std::unique_ptr<VectorRecord>>>>
AsyncCandidateGenerator::submitBatch(const std::vector<const VectorRecord*>& queries, int k) {
  std::vector<std::future<std::vector<std::unique_ptr<VectorRecord>>>> futures;
  futures.reserve(queries.size());

  {
    std::unique_lock<std::mutex> lock(queue_mutex_);

    for (const auto* query : queries) {
      if (query == nullptr) {
        // 为 null 查询创建一个失败的 future
        std::promise<std::vector<std::unique_ptr<VectorRecord>>> promise;
        promise.set_exception(
            std::make_exception_ptr(std::invalid_argument("Query cannot be null")));
        futures.push_back(promise.get_future());
        continue;
      }

      // 如果设置了最大队列大小，等待队列有空间
      if (max_queue_size_ > 0) {
        queue_not_full_.wait(lock, [this] {
          return task_queue_.size() < max_queue_size_ || !running_.load();
        });
      }

      if (!running_.load()) {
        // 生成器已关闭，为剩余查询设置异常
        std::promise<std::vector<std::unique_ptr<VectorRecord>>> promise;
        promise.set_exception(
            std::make_exception_ptr(std::runtime_error("AsyncCandidateGenerator is shutdown")));
        futures.push_back(promise.get_future());
        continue;
      }

      // 复制查询向量
      auto query_copy = std::make_unique<VectorRecord>(*query);
      CandidateQuery cq(std::move(query_copy), k, generateRequestId());
      auto task = std::make_unique<Task>(std::move(cq), nullptr);

      futures.push_back(task->promise.get_future());
      task_queue_.push(std::move(task));
    }
  }

  // 通知多个工作线程
  queue_not_empty_.notify_all();

  return futures;
}

std::future<std::vector<std::unique_ptr<VectorRecord>>>
AsyncCandidateGenerator::submitQueryWithVerification(const VectorRecord& query, int k,
                                                     std::shared_ptr<DistanceVerifier> verifier) {
  // 复制查询向量以确保生命周期安全
  auto query_copy = std::make_unique<VectorRecord>(query);
  CandidateQuery cq(std::move(query_copy), k, generateRequestId());
  auto task = std::make_unique<Task>(std::move(cq), std::move(verifier));

  auto future = task->promise.get_future();

  {
    std::unique_lock<std::mutex> lock(queue_mutex_);

    // 如果设置了最大队列大小，等待队列有空间
    if (max_queue_size_ > 0) {
      queue_not_full_.wait(lock, [this] {
        return task_queue_.size() < max_queue_size_ || !running_.load();
      });
    }

    if (!running_.load()) {
      task->promise.set_exception(
          std::make_exception_ptr(std::runtime_error("AsyncCandidateGenerator is shutdown")));
      return future;
    }

    task_queue_.push(std::move(task));
  }
  queue_not_empty_.notify_one();

  return future;
}

size_t AsyncCandidateGenerator::getPendingCount() const {
  std::lock_guard<std::mutex> lock(queue_mutex_);
  return task_queue_.size();
}

void AsyncCandidateGenerator::shutdown() {
  SAGEFLOW_LOG_INFO("AsyncCandGen", "Initiating graceful shutdown...");

  shutdown_requested_.store(true);

  // 等待队列清空 - 使用轮询方式而不是条件变量
  while (true) {
    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      if (task_queue_.empty()) {
        break;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // 设置 running_ = false 并通知所有工作线程
  running_.store(false);
  queue_not_empty_.notify_all();
  queue_not_full_.notify_all();

  // join 所有线程
  for (auto& worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  workers_.clear();

  SAGEFLOW_LOG_INFO("AsyncCandGen", "Graceful shutdown complete, completed={}", completed_count_.load());
}

void AsyncCandidateGenerator::shutdownNow() {
  SAGEFLOW_LOG_WARN("AsyncCandGen", "Initiating forced shutdown, discarding pending tasks...");

  shutdown_requested_.store(true);
  running_.store(false);

  // 清空队列，为每个待处理任务设置异常
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    while (!task_queue_.empty()) {
      auto task = std::move(task_queue_.front());
      task_queue_.pop();
      task->promise.set_exception(
          std::make_exception_ptr(std::runtime_error("AsyncCandidateGenerator forced shutdown")));
    }
  }

  // 通知所有工作线程
  queue_not_empty_.notify_all();
  queue_not_full_.notify_all();

  // join 所有线程
  for (auto& worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  workers_.clear();

  SAGEFLOW_LOG_WARN("AsyncCandGen", "Forced shutdown complete, completed={}", completed_count_.load());
}

void AsyncCandidateGenerator::workerLoop() {
  SAGEFLOW_LOG_DEBUG("AsyncCandGen", "Worker thread started, tid={}",
                     std::hash<std::thread::id>{}(std::this_thread::get_id()));

  while (true) {
    std::unique_ptr<Task> task;

    {
      std::unique_lock<std::mutex> lock(queue_mutex_);

      // 等待任务或关闭信号
      queue_not_empty_.wait(lock, [this] { return !task_queue_.empty() || !running_.load(); });

      // 检查退出条件
      if (!running_.load() && task_queue_.empty()) {
        break;
      }

      if (task_queue_.empty()) {
        continue;
      }

      task = std::move(task_queue_.front());
      task_queue_.pop();
    }

    // 通知可能在等待队列空间的生产者
    queue_not_full_.notify_one();

    // 执行任务
    try {
      auto result = executeQuery(task->query, task->verifier);
      task->promise.set_value(std::move(result));
    } catch (...) {
      task->promise.set_exception(std::current_exception());
    }

    completed_count_.fetch_add(1);
  }

  SAGEFLOW_LOG_DEBUG("AsyncCandGen", "Worker thread exiting, tid={}",
                     std::hash<std::thread::id>{}(std::this_thread::get_id()));
}

std::vector<std::unique_ptr<VectorRecord>> AsyncCandidateGenerator::executeQuery(
    const CandidateQuery& query, std::shared_ptr<DistanceVerifier> verifier) {
  if (query.query == nullptr) {
    throw std::invalid_argument("Query vector cannot be null");
  }

  if (query.k <= 0) {
    return {};  // k <= 0 时返回空结果
  }

  // 执行索引查询
  std::vector<uint64_t> result_uids = index_->query(*query.query, query.k);

  // 从 storage manager 获取向量记录并转换为 unique_ptr
  std::vector<std::unique_ptr<VectorRecord>> candidates;
  candidates.reserve(result_uids.size());

  if (index_->storage_manager_ != nullptr) {
    for (uint64_t uid : result_uids) {
      auto record_ptr = index_->storage_manager_->getVectorByUid(uid);
      if (record_ptr != nullptr) {
        // record_ptr 是 shared_ptr<const VectorRecord>，需要复制到 unique_ptr
        candidates.push_back(std::make_unique<VectorRecord>(*record_ptr));
      }
    }
  }

  // 如果有验证器，进行验证过滤
  if (verifier != nullptr && !candidates.empty()) {
    candidates = verifier->filterCandidates(*query.query, std::move(candidates));
  }

  return candidates;
}

}  // namespace sageFlow
