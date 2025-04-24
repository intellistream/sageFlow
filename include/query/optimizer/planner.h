#pragma once

#include <memory>

#include "concurrency/concurrency_manager.h"
#include "operator/operator_api.h"

namespace candy {

class Planner {
 public:
  explicit Planner(const std::shared_ptr<ConcurrencyManager>& concurrency_manager);

  auto plan(const std::shared_ptr<Stream>& stream) const -> std::shared_ptr<Operator>;

 private:
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
};
}  // namespace candy