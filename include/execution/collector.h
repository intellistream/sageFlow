//
// Created by ZeroJustMe on 25-7-31.
//

#pragma once

#include "common/data_types.h"
#include <functional>
#include <memory>

namespace candy {
class Collector {
public:
  // 构造函数接收一个可以发射数据的 lambda 函数
  explicit Collector(std::function<void(std::unique_ptr<Response>, int)> emitter)
      : emitter_(std::move(emitter)) {}

  // Operator 调用的核心方法，用于发射一条处理结果
  void collect(std::unique_ptr<Response> record, int slot) const {
    if (emitter_) {
      if (slot == -1) [[unlikely]] { // 如果 slot 为 -1，表示需要广播到所有槽
        if (!slots_.empty()) {
          for (int s : slots_) {
            emitter_(std::make_unique<Response>(*record), s);
          }
        } else {
          // 兼容旧逻辑：以 [0, slot_size_) 广播
          for (int i = 0; i < slot_size_; ++i) {
            emitter_(std::make_unique<Response>(*record), i);
          }
        }
      } else {
        emitter_(std::move(record), slot);
      }
    }
  }

  void set_slot_size(const int size) {
    if (size > 0) {
      slot_size_ = size;
    }
  }

  // 注入该顶点真实可用的 slot 键（非必须从 0 开始）
  void set_slots(std::vector<int> slots) {
    slots_ = std::move(slots);
  }

private:
  std::function<void(std::unique_ptr<Response>, int)> emitter_;
  int slot_size_ = 1;
  std::vector<int> slots_;
};

} // namespace candy