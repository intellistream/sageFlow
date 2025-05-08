#pragma once
#include <functional>
#include <vector>

#include "common/data_types.h"
#include "function/function.h"

namespace candy {

class TopKFunction final : public Function {
 public:
  explicit TopKFunction(const std::string &name);

  TopKFunction(const std::string &name, int k, int index_id);

  auto getK() const -> int;
  auto getIndexId() const -> int;

 private:
  int k_ = 0;
  int index_id_ = 0;
};
};  // namespace candy