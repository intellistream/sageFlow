#pragma once
#include <functional>
#include <vector>

#include "common/data_types.h"
#include "function/function.h"

namespace candy {

class TopkFunction final : public Function {
 public:
  explicit TopkFunction(const std::string &name);

  TopkFunction(const std::string &name, int k, int index_id);

  auto getK() const -> int;
  auto getIndexId() const -> int;

 private:
  int k_ = 0;
  int index_id_ = 0;
};
};  // namespace candy