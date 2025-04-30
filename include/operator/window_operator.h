#pragma once
#include <functional>
#include <list>
#include <memory>

#include "common/data_types.h"
#include "operator/operator.h"

namespace candy {
class WindowOperator : public Operator {
 public:
  explicit WindowOperator(std::unique_ptr<Function> &window_func);

  auto process(Response &data, int slot) -> bool override;
  int window_size_;

 private:
  std::unique_ptr<Function> window_func_;
};

class TumblingWindowOperator final : public WindowOperator {
 public:
  explicit TumblingWindowOperator(std::unique_ptr<Function> &window_func);

  auto process(Response &data, int slot) -> bool override;

 private:
  std::unique_ptr<std::vector<std::unique_ptr<VectorRecord>>> records_;
};

class SlidingWindowOperator final : public WindowOperator {
 public:
  explicit SlidingWindowOperator(std::unique_ptr<Function> &window_func);

  auto process(Response &data, int slot) -> bool override;
  int slide_size_;

  std::list<std::unique_ptr<VectorRecord>> records_;
};
}  // namespace candy