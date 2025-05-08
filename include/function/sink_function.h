#pragma once
#include <functional>

#include "function/function.h"

namespace candy {
using SinkFunc = std::function<void(std::unique_ptr<VectorRecord> &)>;

class SinkFunction final : public Function {
 public:
  explicit SinkFunction(std::string name);

  SinkFunction(std::string name, SinkFunc sink_func);

  auto Execute(DataElement &element) -> DataElement override;

  auto setSinkFunc(SinkFunc sink_func) -> void;

  // Getter for the sink function lambda
  auto getSinkFunction() const -> SinkFunc { return sink_func_; }

 private:
  SinkFunc sink_func_;
};
};  // namespace candy