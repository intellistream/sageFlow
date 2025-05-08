#pragma once

#include <memory>
#include <thread>
#include <vector>

#include "common/data_types.h"
#include "function/function_api.h"
#include "operator/operator_types.h"
#include "stream/elements/stream_element.h"

namespace candy {

// Base class for all operators
class Operator {
 public:
  virtual ~Operator();

  explicit Operator(OperatorType type);

  auto getType() const -> OperatorType;

  virtual auto open() -> void;

  virtual auto close() -> void;

  // 处理任意类型的流元素，返回是否成功处理
  virtual auto process(StreamElement& element, int slot = 0) -> bool;

  // 处理 DataElement - 这是派生类应该重写的主要方法
  virtual auto process(DataElement& element, int slot = 0) -> bool;

  // 将元素发送到指定的下游算子
  virtual void emit(int id, StreamElement& element) const;

  // 将 DataElement 发送到指定的下游算子
  virtual void emit(int id, DataElement& element) const;

  // 为了兼容性，提供处理 DataElement 的方法
  virtual auto processDataElement(DataElement& element, int slot = 0) -> bool;

  auto addChild(std::shared_ptr<Operator> child, int slot = 0) -> int;

  // Check if the operator has received an EndOfStream signal
  bool hasReceivedEndOfStream() const { return received_end_of_stream_; }

  // Getter for is_open_
  bool isOpen() const { return is_open_; }

  // Setter for is_open_
  void setOpen(bool open) { is_open_ = open; }

  // Getter for children_
  const std::vector<std::shared_ptr<Operator>>& getChildren() const { return children_; }

  // Getter for function_
  const std::unique_ptr<Function>& getFunction() const { return function_; }

  // Setter for function_
  void setFunction(std::unique_ptr<Function> function) { function_ = std::move(function); }

 private:
  // Track whether the operator has received an EndOfStream signal
  bool received_end_of_stream_ = false;

  std::vector<int> child2slot_;
  std::unique_ptr<Function> function_ = nullptr;
  std::vector<std::shared_ptr<Operator>> children_;
  OperatorType type_ = OperatorType::NONE;
  bool is_open_ = false;
};

}  // namespace candy
