#pragma once

#include <memory>

#include "operator/operator_api.h"

namespace candy {

class Planner {
 public:
  Planner();

  auto plan(const std::shared_ptr<Stream>& stream) const -> std::shared_ptr<Operator>;
};
}  // namespace candy