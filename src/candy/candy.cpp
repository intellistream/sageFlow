#include "candy/candy.h"

#include <chrono>

#include "common/data_types.h"
#include "compute_engine/compute_engine.h"

namespace candy {

Candy::Candy() {
  auto engine = std::make_shared<ComputeEngine>();
  storage_manager_ = std::make_unique<StorageManager>(engine);
}

auto Candy::insert(int64_t uid, const std::vector<float> &vec) -> void {
  auto dim = static_cast<int32_t>(vec.size());
  auto type = DataType::Float32;
  auto data_size = dim * DATA_TYPE_SIZE[type];
  auto data = std::make_unique<char[]>(data_size);
  std::memcpy(data.get(), vec.data(), data_size);

  VectorData vector_data(dim, type, data.get());

  auto timestamp = std::chrono::system_clock::now().time_since_epoch().count();
  auto record = std::make_unique<VectorRecord>(uid, timestamp, std::move(vector_data));

  storage_manager_->insert(record);
}

auto Candy::erase(int64_t uid) -> bool { return storage_manager_->erase(uid); }

auto Candy::get(int64_t uid) -> std::vector<float> {
  auto record = storage_manager_->getVectorByUid(uid);
  if (record) {
    if (record->data_.type_ != DataType::Float32) {
      return {};
    }
    auto dim = record->data_.dim_;
    std::vector<float> vec(dim);
    std::memcpy(vec.data(), record->data_.data_.get(), dim * sizeof(float));
    return vec;
  }
  return {};
}

auto Candy::topk(const std::vector<float> &vec, int k) -> std::vector<int64_t> {
  auto dim = static_cast<int32_t>(vec.size());
  auto type = DataType::Float32;
  auto data_size = dim * DATA_TYPE_SIZE[type];
  auto data = std::make_unique<char[]>(data_size);
  std::memcpy(data.get(), vec.data(), data_size);
  VectorData vector_data(dim, type, data.get());

  auto record = std::make_unique<VectorRecord>(0, 0, std::move(vector_data));

  auto uids = storage_manager_->topk(record, k);
  std::vector<int64_t> result;
  result.reserve(uids.size());
  for (auto uid_val : uids) {
    result.push_back(static_cast<int64_t>(uid_val));
  }
  return result;
}

}  // namespace candy
