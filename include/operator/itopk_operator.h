#pragma once
#include <functional>
#include <memory>

#include "common/data_types.h"
#include "operator/operator.h"

namespace candy {
class ITopkOperator final : public Operator {
public:
  explicit ITopkOperator(std::unique_ptr<Function> &itopk_func);

  auto process(Response &data, int slot) -> bool override;

private:
  std::unique_ptr<Function> itopk_func_;
};
}  // namespace candy