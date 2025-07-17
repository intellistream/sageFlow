#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "function/source_function.h"
#include "common/data_types.h"

namespace candy {

class FileSourceFunction : public SourceFunction {
 public:
  explicit FileSourceFunction(const std::string& name, std::string file_path)
    : SourceFunction(name), file_path_(std::move(file_path)) {}

  ~FileSourceFunction() override {
    Close();
  }

  void Init() override;
  auto Execute(Response &resp) -> Response override;
  void Close() override;

  void setBufferSizeLimit(size_t limit) { buffer_size_limit_ = limit; }
  auto getBufferSizeLimit() const -> size_t { return buffer_size_limit_; }
  void setTimeout(uint64_t timeout_ms) { timeout_ms_ = timeout_ms; }

 private:
  std::string file_path_;
  std::vector<std::unique_ptr<VectorRecord>> records_;
  std::atomic<bool> running_{false};
  std::atomic<bool> finished_{false};
  uint64_t timeout_ms_{1000};
  size_t buffer_size_limit_{1 << 20};  // 1MB
  std::mutex mtx_;
  std::thread reader_thread_;
};

}  // namespace candy
