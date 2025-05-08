#include "function/filter_function.h"

candy::FilterFunction::FilterFunction(std::string name) : Function(std::move(name), FunctionType::Filter) {}

candy::FilterFunction::FilterFunction(std::string name, FilterFunc filter_func)
    : Function(std::move(name), FunctionType::Filter), filter_func_(std::move(filter_func)) {}

candy::DataElement candy::FilterFunction::Execute(DataElement &element) {
  // 处理单条记录类型
  if (element.isRecord()) {
    // 提取记录并应用过滤函数
    auto record = element.moveRecord();
    
    // 如果通过过滤条件，则返回该记录
    if (filter_func_(record)) {
      return DataElement(std::move(record));
    }
    // 否则返回空元素（表示被过滤掉）
    return DataElement();
  }
  
  // 处理记录列表类型
  if (element.isList()) {
    // 提取记录列表
    auto records = element.moveRecords();
    
    // 应用过滤函数到列表中的每条记录
    for (auto &record : *records) {
      // 对不满足条件的记录设为nullptr（过滤掉）
      if (!filter_func_(record)) {
        record.reset();
      }
    }
    
    // 返回过滤后的记录列表
    return DataElement(std::move(records));
  }
  
  // 对于其他类型或空元素，返回空
  return DataElement();
}

auto candy::FilterFunction::setFilterFunc(FilterFunc filter_func) -> void { 
  filter_func_ = std::move(filter_func); 
}