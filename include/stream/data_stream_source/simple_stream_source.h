#pragma once
#include <atomic>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "common/data_types.h"
#include "stream/data_stream_source/data_stream_source.h"

namespace candy {
class SimpleStreamSource final : public DataStreamSource {
 public:
  explicit SimpleStreamSource(std::string name);

  SimpleStreamSource(std::string name, std::string file_path);

  void Init() override;

  auto Next() -> std::unique_ptr<VectorRecord> override;

  // Programmatic ingestion APIs
  void addRecord(const VectorRecord &rec) {
    records_.push_back(std::make_unique<VectorRecord>(rec));
  }

  void addRecord(uint64_t uid, int64_t timestamp, VectorData &&data) {
    records_.push_back(std::make_unique<VectorRecord>(uid, timestamp, std::move(data)));
  }

 private:
  std::string file_path_;
  std::vector<std::unique_ptr<VectorRecord>> records_;
};
}  // namespace candy