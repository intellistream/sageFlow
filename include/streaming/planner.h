#pragma once

#include <memory>

#include "runtime/function/function.h"
#include "runtime/function/join_function.h"
#include "runtime/operator/operator.h"
#include "runtime/operator/filter_operator.h"
#include "runtime/operator/join_operator.h"
#include "runtime/operator/map_operator.h"
#include "runtime/operator/output_operator.h"
#include "runtime/operator/sink_operator.h"

namespace candy {

class Planner {
 public:
  Planner();

  auto plan(const std::shared_ptr<Stream>& stream) const -> std::shared_ptr<Operator>;
};
}  // namespace candy