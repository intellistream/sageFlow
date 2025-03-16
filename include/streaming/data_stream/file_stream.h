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
   * @class FileStream
   * @brief 用于处理基于文件的数据流的类，继承自 DataStream。
   *
   * FileStream 类负责从文件中读取数据，并提供一个 VectorRecord 对象的队列。
   * 它通过构造函数、析构函数和成员函数支持流的初始化、数据获取和适当的清理。
   */
  class FileStream : public DataStream {
   public:
    /**
     * @brief 使用给定的名称构造 FileStream 对象。
     *
     * 此构造函数使用指定的名称初始化 FileStream，并将数据流类型设置为 File。
     *
     * @param name 数据流的名称。
     */
    explicit FileStream(std::string name);
  
    /**
     * @brief 使用给定的名称和文件路径构造 FileStream 对象。
     *
     * 此构造函数使用指定的名称和文件路径初始化 FileStream，将数据流类型设置为 File，
     * 并将流标记为运行状态。
     *
     * @param name 数据流的名称。
     * @param file_path 要读取数据的文件路径。
     */
    FileStream(std::string name, std::string file_path);
  
    /**
     * @brief FileStream 的析构函数。
     *
     * 析构函数通过将 running_ 标志设置为 false，确保流在对象销毁时停止。
     */
    ~FileStream();
  
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
     * @brief 初始化文件流。
     *
     * 此函数启动一个后台线程，从 file_path_ 指定的文件中读取数据，并将 VectorRecord 对象填充到内部队列中。
     * 该线程将运行直到流停止。
     */
    auto Init() -> void override;
  
   private:
    std::string file_path_;  ///< 正在读取的文件的路径。
    std::atomic<bool> running_;  ///< 指示流是否正在运行的标志。
    std::mutex mtx_;  ///< 保护对 records_ 队列访问的互斥锁。
    std::queue<std::unique_ptr<VectorRecord>> records_;  ///< 存储从文件中读取的 VectorRecord 对象的队列。
  };
  
  }  // namespace candy