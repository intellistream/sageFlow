#include "function/sink_function.h"

candy::SinkFunction::SinkFunction(std::string name) : Function(std::move(name), FunctionType::Sink) {}

candy::SinkFunction::SinkFunction(std::string name, SinkFunc sink_func)
    : Function(std::move(name), FunctionType::Sink), sink_func_(std::move(sink_func)) {}

candy::DataElement candy::SinkFunction::Execute(DataElement &element) {
  // First ensure we have a valid sink function
  if (!sink_func_) {
    // If no sink function is defined, create a new empty DataElement
    // since we can't copy the input element (copy constructor is deleted)
    return DataElement();
  }

  try {
    if (element.isRecord()) {
      auto record = element.moveRecord();
      // Ensure we have a valid record before calling the sink function
      if (record) {
        try {
          sink_func_(record);
        } catch (const std::exception &e) {
          // Log error but don't crash
          // In a real implementation, you would use your logging system
        } catch (...) {
          // Handle unknown exceptions
        }
      }
      return DataElement(std::move(record));
    } else if (element.isList()) {
      auto records = element.moveRecords();
      // Ensure we have a valid records list
      if (records) {
        for (auto &record : *records) {
          // Skip null records in the list
          if (!record) continue;

          try {
            sink_func_(record);
          } catch (const std::exception &e) {
            // Log error but don't crash
            // In a real implementation, you would use your logging system
          } catch (...) {
            // Handle unknown exceptions
          }
        }
      }
      return DataElement(std::move(records));
    }
  } catch (const std::exception &e) {
    // Log the exception but don't crash
    // In a real implementation, you would use your logging system
  } catch (...) {
    // Handle unknown exceptions
  }

  // Return empty data element if input was invalid or any exception occurred
  return DataElement();
}

auto candy::SinkFunction::setSinkFunc(SinkFunc sink_func) -> void { sink_func_ = std::move(sink_func); }