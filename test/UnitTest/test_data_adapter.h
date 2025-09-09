#pragma once
#include "common/data_types.h"
#include <vector>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <cstring>

namespace candy {
namespace test {

// 测试专用的简化数据结构，适配现有的VectorRecord接口
// 注意：Join 的左右侧由 apply 的 slot(0/1) 决定；下面的 Side 仅用于
// 基线校验（BaselineJoinChecker）中区分左右侧，不影响算子逻辑。
enum class Side { LEFT, RIGHT, BOTH };

// 创建VectorRecord的辅助函数
inline std::unique_ptr<VectorRecord> createVectorRecord(
    uint64_t uid, int64_t timestamp, const std::vector<float>& data) {
  
  // 将float向量转换为char数组
  int32_t dim = static_cast<int32_t>(data.size());
  auto raw_data = std::make_unique<char[]>(dim * sizeof(float));
  std::memcpy(raw_data.get(), data.data(), dim * sizeof(float));
  
  VectorData vector_data(dim, DataType::Float32, raw_data.release());
  
  return std::make_unique<VectorRecord>(uid, timestamp, std::move(vector_data));
}

// 从VectorRecord中提取float向量的辅助函数
inline std::vector<float> extractFloatVector(const VectorRecord& record) {
  const auto& vector_data = record.data_;
  int32_t dim = vector_data.dim_;
  if (dim <= 0 || dim > 1000000) {
    // 仅诊断日志：维度异常，便于定位；不改变行为
    std::cerr << "[DIAG] extractFloatVector abnormal dim: " << dim
              << ", uid=" << record.uid_ << ", ts=" << record.timestamp_ << std::endl;
  }
  const float* float_ptr = reinterpret_cast<const float*>(vector_data.data_.get());
  
  std::vector<float> result(dim);
  std::memcpy(result.data(), float_ptr, dim * sizeof(float));
  return result;
}

// 测试专用的Side信息存储（因为VectorRecord不包含side字段）
class TestRecordSideManager {
public:
  static TestRecordSideManager& instance() {
    static TestRecordSideManager inst;
    return inst;
  }
  
  void setSide(uint64_t uid, Side side) {
    std::lock_guard<std::mutex> lock(mutex_);
    uid_to_side_[uid] = side;
  }
  
  Side getSide(uint64_t uid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = uid_to_side_.find(uid);
    return it != uid_to_side_.end() ? it->second : Side::LEFT;
  }
  
  void clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    uid_to_side_.clear();
  }
  
private:
  mutable std::mutex mutex_;
  std::unordered_map<uint64_t, Side> uid_to_side_;
};

} // namespace test
} // namespace candy