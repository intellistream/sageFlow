#pragma once
#include <functional>
#include <memory>

#include "common/data_types.h"
#include "operator/operator.h"

namespace candy {
class WindowOperator  : public Operator {
 public:
  explicit WindowOperator(std::unique_ptr<Function> &window_func);

  auto process(Response &data, int slot) -> bool override;

 private:
  std::unique_ptr<Function> window_func_;
};

class TumblingWindowOperator final : public WindowOperator {
 public:
  explicit TumblingWindowOperator(std::unique_ptr<Function> &window_func);

  auto process(Response &data, int slot) -> bool override;
};

class SlidingWindowOperator final : public WindowOperator {
 public:
  explicit SlidingWindowOperator(std::unique_ptr<Function> &window_func);

  auto process(Response &data, int slot) -> bool override;
};
}  // namespace candy