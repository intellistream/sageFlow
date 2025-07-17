#pragma once

#include <string>

#include "function/function.h"
#include "common/data_types.h"

namespace candy {

class SourceFunction : public Function {
 public:
  explicit SourceFunction(const std::string& name) 
    : Function(name, FunctionType::Source) {}

  ~SourceFunction() override = default;

  // Initialize the source (e.g., open files, connect to streams)
  virtual void Init() = 0;

  // Execute returns a batch of records. Empty vector means no more data.
  // This replaces the need for separate Next() and HasNext() methods.
  auto Execute(Response &resp) -> Response override = 0;

  // Close the source
  virtual void Close() = 0;
};

}  // namespace candy
