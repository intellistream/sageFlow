#include "test_utils/datasource_modes/splitter.h"

#include <algorithm>

#include "utils/logger.h"

namespace sageFlow {
namespace test {

std::unique_ptr<VectorRecord> copyWithOffset(const VectorRecord& record, uint64_t offset) {
  return std::make_unique<VectorRecord>(
      record.uid_ + offset, record.timestamp_, record.data_);
}

SplitRecords splitDatasourceRecords(
    std::vector<std::unique_ptr<VectorRecord>> base_records,
    const std::string& split_mode,
    uint64_t right_uid_offset) {
  SplitRecords split;

  if (split_mode == "half_split") {
    const size_t half = base_records.size() / 2;
    split.left.reserve(half);
    split.right.reserve(base_records.size() - half);
    for (size_t i = 0; i < base_records.size(); ++i) {
      if (i < half) {
        split.left.push_back(std::move(base_records[i]));
      } else if (base_records[i]) {
        split.right.push_back(copyWithOffset(*base_records[i], right_uid_offset));
      }
    }
    SAGEFLOW_LOG_INFO("TEST", "[SPLIT] half_split mode: left={} right={}",
                      split.left.size(), split.right.size());
    return split;
  }

  if (split_mode == "interleaved") {
    split.left.reserve(base_records.size() / 2 + 1);
    split.right.reserve(base_records.size() / 2 + 1);
    for (size_t i = 0; i < base_records.size(); ++i) {
      if (i % 2 == 0) {
        split.left.push_back(std::move(base_records[i]));
      } else if (base_records[i]) {
        split.right.push_back(copyWithOffset(*base_records[i], right_uid_offset));
      }
    }
    SAGEFLOW_LOG_INFO("TEST", "[SPLIT] interleaved mode: left={} right={}",
                      split.left.size(), split.right.size());
    return split;
  }

  split.left.reserve(base_records.size());
  for (auto& record : base_records) {
    split.left.push_back(std::move(record));
  }
  split.right.reserve(split.left.size());
  for (const auto& record : split.left) {
    if (record) {
      split.right.push_back(copyWithOffset(*record, right_uid_offset));
    }
  }
  SAGEFLOW_LOG_INFO("TEST", "[SPLIT] duplicate mode: left={} right={}",
                    split.left.size(), split.right.size());
  return split;
}

}  // namespace test
}  // namespace sageFlow
