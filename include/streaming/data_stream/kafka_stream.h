#pragma once
#include <atomic>
#include <queue>
#include <string>
#include <mutex>
#include <memory>

#include "core/common/data_types.h"
#include "proto/message.pb.h"
#include "streaming/data_stream/data_stream.h"
#include <librdkafka/rdkafkacpp.h>  // Kafka C++ client library

namespace candy {

  /**
   * @class KafkaStream
   * @brief 用于处理基于 Kafka 消息队列的数据流的类，继承自 DataStream。
   *
   * KafkaStream 类负责从 Kafka 消息队列中读取数据，并提供一个 VectorRecord 对象的队列。
   * 它通过构造函数、析构函数和成员函数支持流的初始化、数据获取和适当的清理。
   */
  class KafkaStream : public DataStream {
   public:
    /**
     * @brief 使用给定的名称、Kafka broker 和 topic 构造 KafkaStream 对象。
     *
     * 此构造函数使用指定的名称、Kafka broker 和 topic 初始化 KafkaStream，将数据流类型设置为 Kafka，
     * 并将流标记为运行状态。
     *
     * @param name 数据流的名称。
     * @param broker Kafka broker 地址。
     * @param topic Kafka topic 名称。
     */
    KafkaStream(std::string name, std::string broker, std::string topic);

    /**
     * @brief KafkaStream 的析构函数。
     *
     * 析构函数通过将 running_ 标志设置为 false，确保流在对象销毁时停止，并关闭 Kafka 消费者。
     */
    ~KafkaStream();

    /**
     * @brief 从流中获取下一个 VectorRecord。
     *
     * 此函数尝试从内部队列中获取下一个可用的 VectorRecord。如果队列为空，则返回 false；
     * 否则，将记录移动到提供的指针并返回 true。
     *
     * @param record 用于保存下一个 VectorRecord 的 unique_ptr 引用。
     * @return 如果成功获取记录则返回 true，如果队列为空则返回 false。
     */
    auto Next(RecordOrWatermark &record_or_watermark) -> bool override;

    /**
     * @brief 初始化 Kafka 流。
     *
     * 此函数启动一个后台线程，连接到 Kafka broker，订阅指定的 topic，从中读取数据，
     * 并将 VectorRecord 对象填充到内部队列中。该线程将运行直到流停止。
     */
    auto Init() -> void override;

   private:
    std::string broker_;  ///< Kafka broker 地址。
    std::string topic_;  ///< Kafka topic 名称。
    std::atomic<bool> running_;  ///< 指示流是否正在运行的标志。
    std::mutex mtx_;  ///< 保护对 records_ 队列访问的互斥锁。
    std::queue<std::unique_ptr<VectorRecord>> records_;  ///< 存储从 Kafka 消息中读取的 VectorRecord 对象的队列。
    std::string errstr_;  //< Error string for Kafka configuration
    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;  ///< Kafka 消费者对象。
  };

}  // namespace candy