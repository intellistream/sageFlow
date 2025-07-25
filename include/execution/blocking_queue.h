//
// Created by ZeroJustMe on 25-7-22.
//

#pragma once

#include <queue>
#include <mutex>
#include <condition_variable>
#include <optional>
#include <utility> // for std::move
#include "common/data_types.h"

namespace candy {
class BlockingQueue {
public:
    /**
     * @brief 构造一个有界阻塞队列。
     * @param max_size 队列的最大容量，用于实现反压。
     */
    explicit BlockingQueue(size_t max_size) : max_size_(max_size), stopped_(false) {}

    /**
     * @brief [生产者调用] 将一个元素推入队列。
     * 如果队列已满，此方法会阻塞，直到队列有可用空间或被停止。
     * 这是线程安全的，可以被多个生产者线程同时调用。
     * @param value 要推入队列的元素。
     */
    void push(Response value);

    /**
     * @brief [消费者调用] 从队列中弹出一个元素。
     * 如果队列为空，此方法会阻塞，直到队列中有元素或被停止。
     * @return std::optional<T> 包含弹出的元素；如果队列已停止且为空，则返回 std::nullopt。
     */
    std::optional<Response> pop();

    /**
     * @brief 停止队列。
     * 这会唤醒所有正在等待的生产者和消费者线程，
     * 并使后续的 push 调用立即返回，pop 调用在队列为空后返回 std::nullopt。
     */
    void stop() ;

private:
    const size_t max_size_;
    std::queue<Response> queue_;
    std::mutex mutex_;
    std::condition_variable cond_not_empty_; // 消费者在此等待
    std::condition_variable cond_not_full_;  // 生产者在此等待
    std::atomic<bool> stopped_;
};

}