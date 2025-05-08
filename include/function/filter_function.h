#pragma once
#include <functional>

#include "function/function.h"

namespace candy {
// 使用函数对象类型来表示过滤条件
using FilterFunc = std::function<bool(std::unique_ptr<VectorRecord> &)>;

class FilterFunction final : public Function {
 public:
  explicit FilterFunction(std::string name);

  FilterFunction(std::string name, FilterFunc filter_func);

  // 更新 Execute 方法以接受 DataElement
  auto Execute(DataElement &element) -> DataElement override;

  auto setFilterFunc(FilterFunc filter_func) -> void;

 private:
  FilterFunc filter_func_;
};
}  // namespace candy