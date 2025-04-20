#pragma once
#include <functional>

#include "function/function.h"

namespace candy {
using SinkFunc = std::function<void(std::unique_ptr<VectorRecord> &)>;

class SinkFunction final : public Function {
 public:
  explicit SinkFunction(std::string name);

  SinkFunction(std::string name, SinkFunc sink_func);

  auto Execute(Response &resp) -> Response override;

  auto setSinkFunc(SinkFunc sink_func) -> void;

 private:
  SinkFunc sink_func_;
};
};  // namespace candy