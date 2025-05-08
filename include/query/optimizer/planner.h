#pragma once

#include <memory>

#include "concurrency/concurrency_manager.h"
#include "operator/operator_api.h"

namespace candy {

class Planner {
 public:
  // Changed to take a reference instead of shared_ptr
  explicit Planner(ConcurrencyManager& concurrency_manager);

  // Keep this as shared_ptr since streams are shared by design
  auto plan(const std::shared_ptr<Stream>& stream) const -> std::shared_ptr<Operator>;

 private:
  // Hold a reference instead of shared_ptr
  ConcurrencyManager& concurrency_manager_;
};
}  // namespace candy