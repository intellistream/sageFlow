#pragma once

#include <memory>
#include <vector>

#include "storage/storage_manager.h"

namespace candy {

class Candy {
 public:
  Candy();
  ~Candy() = default;

  // 禁止拷贝和移动
  Candy(const Candy &) = delete;
  auto operator=(const Candy &) -> Candy & = delete;
  Candy(Candy &&) = delete;
  auto operator=(Candy &&) -> Candy & = delete;

  auto insert(int64_t uid, const std::vector<float> &vec) -> void;
  auto erase(int64_t uid) -> bool;
  auto get(int64_t uid) -> std::vector<float>;
  auto topk(const std::vector<float> &vec, int k) -> std::vector<int64_t>;

 private:
  std::unique_ptr<StorageManager> storage_manager_;
};

}  // namespace candy
