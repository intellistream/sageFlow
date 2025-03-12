#pragma once
#include <utility>
#include <vector>

#include "core/common/data_types.h"
#include "streaming/data_stream_source/data_stream_source.h"

namespace candy {
class FileStreamSource : public DataStreamSource {
 public:
  explicit FileStreamSource(std::string name) : DataStreamSource(std::move(name), DataStreamSourceType::File) {}

  FileStreamSource(std::string name, std::string file_path)
      : DataStreamSource(std::move(name), DataStreamSourceType::File), file_path_(std::move(file_path)) {}

  void Init() override {
    records_.clear();
    records_.emplace_back(std::make_unique<VectorRecord>("1", VectorData{1.0, 2.0, 3.0}, 0));
    records_.emplace_back(std::make_unique<VectorRecord>("2", VectorData{4.0, 5.0, 6.0}, 1));
    records_.emplace_back(std::make_unique<VectorRecord>("3", VectorData{7.0, 8.0, 9.0}, 2));
  }

  auto Next() -> std::unique_ptr<VectorRecord> override {
    if (records_.empty()) {
      // load from file
    }
    if (records_.empty()) {
      return nullptr;
    }
    auto record = std::move(records_.back());
    records_.pop_back();
    return record;
  }

 private:
  std::string file_path_;
  std::vector<std::unique_ptr<VectorRecord>> records_;
};
}  // namespace candy