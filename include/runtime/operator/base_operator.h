
#pragma once

#include <core/common/data_types.h>  // Include VectorRecord definition
#include <runtime/function/function.h>

#include <memory>
#include <thread>
#include <vector>

namespace candy {
enum class OperatorType { NONE, OUTPUT, FILTER, MAP, JOIN, SINK };

// Base class for all operators
class Operator {
 public:
  virtual ~Operator() = default;

  explicit Operator(const OperatorType type) : type_(type) {}

  auto getType() const -> OperatorType { return type_; }

  virtual auto open() -> void {
    if (is_open_) {
      return;
    }
    is_open_ = true;
    for (const auto& child : children_) {
      child->open();
    }
  }

  virtual auto close() -> void {}

  virtual auto process(std::unique_ptr<VectorRecord>& record, int slot = 0) -> bool {
    emit(0, record);
    return true;
  }

  virtual void emit(const int id, std::unique_ptr<VectorRecord>& record) const {
    if (children_.empty()) {
      return;
    }
    int slot = child2slot_[id];
    children_[id]->process(record, slot);
  }

  auto addChild(std::shared_ptr<Operator> child, const int slot = 0) -> int {
    children_.push_back(std::move(child));
    const int index = children_.size() - 1;
    child2slot_.push_back(slot);
    return index;
  }

  std::vector<int> child2slot_;
  std::unique_ptr<Function> function_ = nullptr;
  std::vector<std::shared_ptr<Operator>> children_;
  OperatorType type_ = OperatorType::NONE;
  bool is_open_ = false;
};

}  // namespace candy
