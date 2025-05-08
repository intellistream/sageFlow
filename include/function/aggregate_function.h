#pragma once
#include <functional>
#include <vector>

#include "common/data_types.h"
#include "function/function.h"

namespace candy {

enum class AggregateType { None, Avg };

class AggregateFunction final : public Function {
 public:
  explicit AggregateFunction(const std::string &name);

  AggregateFunction(const std::string &name, AggregateType aggregate_type);
  auto getAggregateType() const -> AggregateType;

 private:
  AggregateType aggregate_type_ = AggregateType::None;
};
};  // namespace candy