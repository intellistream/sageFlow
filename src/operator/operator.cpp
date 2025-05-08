#include "operator/operator.h"

candy::Operator::~Operator() = default;

candy::Operator::Operator(const OperatorType type) : type_(type) {}

auto candy::Operator::getType() const -> OperatorType { return type_; }

auto candy::Operator::open() -> void {
  if (is_open_) {
    return;
  }
  is_open_ = true;
  for (const auto& child : children_) {
    child->open();
  }
}

auto candy::Operator::close() -> void {}

// 处理通用流元素
auto candy::Operator::process(StreamElement& element, int slot) -> bool {
  // 根据元素类型分发到不同的处理逻辑
  switch (element.getType()) {
    case StreamElementType::DATA: {
      // 如果是数据元素，我们尝试将其转换为 DataElement
      auto* dataElement = dynamic_cast<DataElement*>(&element);
      if (dataElement) {
        return processDataElement(*dataElement, slot);
      }
      // 无法转换时返回失败
      return false;
    }
    
    case StreamElementType::WATERMARK: {
      // 水位线处理逻辑（默认直接传递给所有子算子）
      for (size_t i = 0; i < children_.size(); ++i) {
        emit(i, element);
      }
      return true;
    }
    
    case StreamElementType::END_OF_STREAM: {
      // 结束流信号处理逻辑（默认直接传递给所有子算子）
      for (size_t i = 0; i < children_.size(); ++i) {
        emit(i, element);
      }
      return true;
    }
    
    default:
      // 未知元素类型，默认传递给下游
      emit(0, element);
      return true;
  }
}

// 具体处理数据元素的方法（子类可重写此方法）
auto candy::Operator::processDataElement(DataElement& element, int slot) -> bool {
  // 默认实现：直接将数据元素发送给第一个子算子
  emit(0, element);
  return true;
}

// Process DataElement specifically (default implementation)
auto candy::Operator::process(DataElement& element, int slot) -> bool {
  // Default implementation just forwards to processDataElement
  return processDataElement(element, slot);
}

// 发送元素到指定的子算子
void candy::Operator::emit(const int id, StreamElement& element) const {
  if (children_.empty() || id >= children_.size()) {
    return;
  }
  int slot = child2slot_[id];
  children_[id]->process(element, slot);
}

// Send a DataElement to the specified downstream operator
void candy::Operator::emit(const int id, DataElement& element) const {
  if (children_.empty() || id >= children_.size()) {
    return;
  }
  int slot = child2slot_[id];
  children_[id]->process(element, slot);
}

auto candy::Operator::addChild(std::shared_ptr<Operator> child, const int slot) -> int {
  children_.push_back(std::move(child));
  const int index = children_.size() - 1;
  child2slot_.push_back(slot);
  return index;
}