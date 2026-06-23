#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/data_types.h"

namespace sageFlow {
namespace test {

constexpr uint64_t kDatasourceRightUidOffset = 500000ULL;
constexpr uint64_t kDatasourceModuloBase = 1000000ULL;

struct SplitRecords {
  std::vector<std::unique_ptr<VectorRecord>> left;
  std::vector<std::unique_ptr<VectorRecord>> right;
};

SplitRecords splitDatasourceRecords(
    std::vector<std::unique_ptr<VectorRecord>> base_records,
    const std::string& split_mode,
    uint64_t right_uid_offset = kDatasourceRightUidOffset);

}  // namespace test
}  // namespace sageFlow
