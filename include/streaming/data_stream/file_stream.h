#pragma once
#include <utility>
#include <vector>

#include "core/common/data_types.h"
#include "streaming/data_stream/data_stream.h"

namespace candy {
class FileStream : public DataStream {
 public:
  explicit FileStream(std::string name) : DataStream(std::move(name), DataFlowType::File) {}

  FileStream(std::string name, std::string file_path)
      : DataStream(std::move(name), DataFlowType::File), file_path_(std::move(file_path)) {}

  auto Next(std::unique_ptr<VectorRecord>& record) -> bool override {
    if (records_.empty()) {
      return false;
    }
    record = std::move(records_.back());
    records_.pop_back();
    return true;
  }

  auto Init() -> void override {
    // Read file and populate records_
    records_.emplace_back(std::make_unique<VectorRecord>("1", VectorData{1.0, 2.0, 3.0}, 0));
    records_.emplace_back(std::make_unique<VectorRecord>("2", VectorData{4.0, 5.0, 6.0}, 1));
    records_.emplace_back(std::make_unique<VectorRecord>("3", VectorData{7.0, 8.0, 9.0}, 2));
  }

 private:
  std::string file_path_;
  std::vector<std::unique_ptr<VectorRecord>> records_;
};
}  // namespace candy