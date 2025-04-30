#pragma once
#include <utility>
#include <deque>
#include <atomic>
#include <mutex>
#include <thread>

#include "common/data_types.h"
#include "stream/data_stream_source/data_stream_source.h"

namespace candy {
class FileStreamSource : public DataStreamSource {
 public:
  explicit FileStreamSource(std::string name);

  FileStreamSource(std::string name, std::string file_path);

  void Init() override;

  auto Next() -> std::unique_ptr<VectorRecord> override;

 private:
  std::string file_path_;
  std::deque<std::unique_ptr<VectorRecord>> records_;  // Using deque for efficient FIFO operations
  std::atomic<bool> running_{false};
  uint64_t timeout_ms_{1000};
  std::mutex mtx_;
};
}  // namespace candy