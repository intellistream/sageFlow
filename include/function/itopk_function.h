#pragma once

#pragma once
#include <functional>
#include <vector>

#include "common/data_types.h"
#include "function/function.h"

namespace candy {

class ITopkFunction final : public Function {
 public:
  explicit ITopkFunction(const std::string &name);

  ITopkFunction(const std::string &name, int k);

  auto getK() const -> int;

 private:
  int k_ = 0;
  int index_id_ = 0;
};
};  // namespace candy
