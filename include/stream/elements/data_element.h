#pragma once

#include <memory>
#include <vector>

#include "common/vector_record.h"
#include "stream/elements/stream_element.h"

namespace candy {

/**
 * @brief 表示流中的数据元素
 * 
 * DataElement 可以封装单个 VectorRecord 或多个 VectorRecord 列表，
 * 提供安全的内存管理和所有权语义。它替代了原来的 Response 类的功能。
 */
class DataElement : public StreamElement {
public:
    /**
     * @brief 数据元素类型枚举 (内部使用)
     */
    enum class ElementType {
        None,    // 空数据元素
        Record,  // 单条记录
        List     // 记录列表
    };

    // 默认构造函数 - 创建空数据元素
    DataElement() : StreamElement(StreamElementType::DATA), element_type_(ElementType::None) {}

    // 构造单条记录的数据元素
    explicit DataElement(std::unique_ptr<VectorRecord> record)
        : StreamElement(StreamElementType::DATA), 
          element_type_(record ? ElementType::Record : ElementType::None), 
          record_(std::move(record)) {
    }

    // 构造记录列表的数据元素
    explicit DataElement(std::unique_ptr<std::vector<std::unique_ptr<VectorRecord>>> records)
        : StreamElement(StreamElementType::DATA), 
          element_type_(records && !records->empty() ? ElementType::List : ElementType::None), 
          records_(std::move(records)) {
    }

    // 禁止拷贝，允许移动
    DataElement(const DataElement&) = delete;
    DataElement& operator=(const DataElement&) = delete;
    
    // 移动构造函数
    DataElement(DataElement&& other) noexcept
        : StreamElement(StreamElementType::DATA),
          element_type_(other.element_type_),
          record_(std::move(other.record_)),
          records_(std::move(other.records_)) {
        other.element_type_ = ElementType::None;
    }
    
    // 移动赋值运算符
    DataElement& operator=(DataElement&& other) noexcept {
        if (this != &other) {
            element_type_ = other.element_type_;
            record_ = std::move(other.record_);
            records_ = std::move(other.records_);
            other.element_type_ = ElementType::None;
        }
        return *this;
    }
    
    // 正确重写基类的 getType() 方法
    auto getType() const -> StreamElementType override { 
        return StreamElementType::DATA;  // 始终返回 DATA 类型 
    }
    
    // 获取数据元素内部类型
    ElementType getElementType() const { return element_type_; }
    
    // 判断是否为空
    bool isEmpty() const { return element_type_ == ElementType::None; }
    
    // 判断是否为单条记录
    bool isRecord() const { return element_type_ == ElementType::Record && record_ != nullptr; }
    
    // 判断是否为记录列表
    bool isList() const { return element_type_ == ElementType::List && records_ != nullptr && !records_->empty(); }
    
    // 访问单条记录 (只读)
    const VectorRecord* getRecord() const { 
        return (element_type_ == ElementType::Record && record_) ? record_.get() : nullptr; 
    }
    
    // 访问单条记录 (可修改)
    VectorRecord* getRecord() { 
        return (element_type_ == ElementType::Record && record_) ? record_.get() : nullptr; 
    }
    
    // 访问记录列表 (只读)
    const std::vector<std::unique_ptr<VectorRecord>>* getRecords() const {
        return (element_type_ == ElementType::List && records_) ? records_.get() : nullptr;
    }
    
    // 访问记录列表 (可修改)
    std::vector<std::unique_ptr<VectorRecord>>* getRecords() {
        return (element_type_ == ElementType::List && records_) ? records_.get() : nullptr;
    }
    
    // 转移记录所有权
    std::unique_ptr<VectorRecord>&& moveRecord() { 
        element_type_ = ElementType::None;
        return std::move(record_); 
    }
    
    // 转移记录列表所有权
    std::unique_ptr<std::vector<std::unique_ptr<VectorRecord>>>&& moveRecords() {
        element_type_ = ElementType::None;
        return std::move(records_);
    }

    // 便捷方法 (适用于单条记录)
    uint64_t getUid() const { 
        return (element_type_ == ElementType::Record && record_) ? record_->uid_ : 0; 
    }
    
    timestamp_t getEventTime() const { 
        return (element_type_ == ElementType::Record && record_) ? record_->timestamp_ : -1; 
    }
    
    const VectorData* getVectorData() const { 
        return (element_type_ == ElementType::Record && record_) ? &record_->data_ : nullptr; 
    }
    
private:
    ElementType element_type_ = ElementType::None;      // 数据元素内部类型
    std::unique_ptr<VectorRecord> record_;             // 单条记录
    std::unique_ptr<std::vector<std::unique_ptr<VectorRecord>>> records_;  // 记录列表
};

}  // namespace candy