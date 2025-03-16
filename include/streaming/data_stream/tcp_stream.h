#pragma once
#include <atomic>
#include <queue>
#include <string>
#include <utility>

#include "core/common/data_types.h"
#include "proto/message.pb.h"
#include "streaming/data_stream/data_stream.h"

namespace candy {

  /**
   * @class TcpStream
   * @brief 用于处理基于 TCP 连接的数据流的类，继承自 DataStream。
   *
   * TcpStream 类负责从 TCP 连接中读取数据，并提供一个 VectorRecord 对象的队列。
   * 它通过构造函数、析构函数和成员函数支持流的初始化、数据获取和适当的清理。
   */
  class TcpStream : public DataStream {
   public:
    /**
     * @brief 使用给定的名称构造 TcpStream 对象。
     *
     * 此构造函数使用指定的名称初始化 TcpStream，并将数据流类型设置为 Tcp。
     *
     * @param name 数据流的名称。
     */
    explicit TcpStream(std::string name);
  
    /**
     * @brief 使用给定的名称、IP 地址和端口号构造 TcpStream 对象。
     *
     * 此构造函数使用指定的名称、IP 地址和端口号初始化 TcpStream，将数据流类型设置为 Tcp，
     * 并将流标记为运行状态。
     *
     * @param name 数据流的名称。
     * @param ip_address TCP 连接的 IP 地址。
     * @param port TCP 连接的端口号。
     */
    TcpStream(std::string name, std::string ip_address, int port);
  
    /**
     * @brief TcpStream 的析构函数。
     *
     * 析构函数通过将 running_ 标志设置为 false，确保流在对象销毁时停止，并关闭 TCP 连接。
     */
    ~TcpStream();
  
    /**
     * @brief 从流中获取下一个 VectorRecord。
     *
     * 此函数尝试从内部队列中获取下一个可用的 VectorRecord。如果队列为空，则返回 false；
     * 否则，将记录移动到提供的指针并返回 true。
     *
     * @param record 用于保存下一个 VectorRecord 的 unique_ptr 引用。
     * @return 如果成功获取记录则返回 true，如果队列为空则返回 false。
     */
    auto Next(std::unique_ptr<VectorRecord>& record) -> bool override;
  
    /**
     * @brief 初始化 TCP 流。
     *
     * 此函数启动一个后台线程，建立 TCP 连接，从 ip_address_ 和 port_ 指定的服务器中读取数据，
     * 并将 VectorRecord 对象填充到内部队列中。该线程将运行直到流停止。
     */
    auto Init() -> void override;
  
   private:
    std::string ip_address_;  ///< TCP 连接的 IP 地址。
    int port_;  ///< TCP 连接的端口号。
    std::atomic<bool> running_;  ///< 指示流是否正在运行的标志。
    std::mutex mtx_;  ///< 保护对 records_ 队列访问的互斥锁。
    std::queue<std::unique_ptr<VectorRecord>> records_;  ///< 存储从 TCP 连接中读取的 VectorRecord 对象的队列。
  };
  
  }  // namespace candy