//
// Created by ZeroJustMe on 25-7-22.
//

#include "execution/blocking_queue.h"

namespace candy {
void BlockingQueue::push(Response value) {
  std::unique_lock<std::mutex> lock(mutex_);

  // 等待直到队列有空间 (不满) 或者队列被停止
  // 使用 lambda 谓词可以完美处理虚假唤醒
  cond_not_full_.wait(lock, [this] { return queue_.size() < max_size_ || stopped_; });

  // 如果队列被停止，则直接返回，不再推入新元素
  if (stopped_) {
    return;
  }

  queue_.push(std::move(value));

  // 唤醒一个可能正在等待队列不为空的消费者线程
  cond_not_empty_.notify_one();
}

std::optional<Response> BlockingQueue::pop() {
  std::unique_lock<std::mutex> lock(mutex_);

  // 等待直到队列不为空或者队列被停止
  cond_not_empty_.wait(lock, [this] { return !queue_.empty() || stopped_; });

  // 如果队列被停止且已经没有元素可消费，则返回空 optional 作为结束信号
  if (stopped_ && queue_.empty()) {
    return std::nullopt;
  }

  Response value = std::move(queue_.front());
  queue_.pop();

  // 唤醒一个可能正在等待队列有空间的生产者线程
  cond_not_full_.notify_one();
  return value;
}

void BlockingQueue::stop() {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    stopped_ = true;
  }
  // 必须唤醒所有线程，以便它们能够重新检查 stopped_ 标志并退出
  cond_not_empty_.notify_all();
  cond_not_full_.notify_all();
}
}