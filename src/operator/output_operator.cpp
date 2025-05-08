#include "operator/output_operator.h"

#include <iostream>
#include <spdlog/spdlog.h>

using namespace candy;

OutputOperator::OutputOperator() : Operator(OperatorType::OUTPUT) {}

OutputOperator::OutputOperator(const OutputChoice output_choice, std::shared_ptr<DataStreamSource> stream)
    : Operator(OperatorType::OUTPUT), output_choice_(output_choice), stream_(std::move(stream)) {}

OutputOperator::OutputOperator(std::shared_ptr<DataStreamSource> stream)
    : Operator(OperatorType::OUTPUT), stream_(std::move(stream)) {}

void OutputOperator::open() {
  if (isOpen()) {
    spdlog::warn("OutputOperator already open");
    return;
  }
  setOpen(true);
  stream_->Init();
  for (const auto& child : getChildren()) {
    spdlog::info("Child operator type: {}", static_cast<int>(child->getType()));
    child->open();
  }
  std::unique_ptr<VectorRecord> record = nullptr;
  while ((record = stream_->Next())) {
    auto element = DataElement(std::move(record));
    process(element);
  }
}

bool OutputOperator::process(DataElement &element, int slot) {
  // Process incoming data - this is an output operator, so it emits data to all children operators
  
  // Don't attempt to make a copy - use move semantics instead
  if (element.isRecord() || element.isList()) {
    // If we have multiple children to emit to, create moved copies for each but the last one
    for (size_t i = 0; i < getChildren().size() - 1; ++i) {
      DataElement moveableCopy = createMovedElement(element);
      emit(i, moveableCopy);
    }
    
    // For the last child (or if there's only one), use the original element
    if (!getChildren().empty()) {
      emit(getChildren().size() - 1, element);
    }
  }

  return true;
}

DataElement OutputOperator::createMovedElement(const DataElement &original) {
  // Create a properly moved copy for each output child
  if (original.isRecord() && original.getRecord()) {
    // For records, we need to clone the vector record
    std::unique_ptr<VectorRecord> clonedRecord = cloneVectorRecord(*original.getRecord());
    return DataElement(std::move(clonedRecord));
  } else if (original.isList() && original.getRecords()) {
    // For record lists, clone each record in the list
    auto clonedRecords = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>();
    for (const auto &record : *original.getRecords()) {
      if (record) {
        clonedRecords->push_back(cloneVectorRecord(*record));
      }
    }
    return DataElement(std::move(clonedRecords));
  }
  
  // Default empty element
  return DataElement();
}

std::unique_ptr<VectorRecord> OutputOperator::cloneVectorRecord(const VectorRecord &record) {
  // Create a clone of the vector data
  VectorData clonedData(record.data_);
  
  // Return a new vector record with the cloned data
  return std::make_unique<VectorRecord>(record.uid_, record.timestamp_, std::move(clonedData));
}