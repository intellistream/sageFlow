#pragma once
#include <utility>
#include <queue>
#include <atomic>
#include <string>
#include "core/common/data_types.h"
#include "streaming/data_stream/data_stream.h"
#include "proto/message.pb.h"

namespace candy {
class FileStream : public DataStream {
 public:
  explicit FileStream(std::string name) : DataStream(std::move(name), DataFlowType::File) {}

  FileStream(std::string name, std::string file_path)
      : DataStream(std::move(name), DataFlowType::File), file_path_(std::move(file_path)),running_(true) {}
  ~FileStream() = default;
  auto Next(std::unique_ptr<VectorRecord>& record) -> bool override;

  auto Init() -> void override;

 private:
  std::string file_path_;
  std::atomic<bool> running_;
  std::mutex mtx_;
  std::queue<std::unique_ptr<VectorRecord>> records_;
};
}  // namespace candy