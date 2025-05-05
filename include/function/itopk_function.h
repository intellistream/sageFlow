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

  ITopkFunction(const std::string &name, int k, int dim, std::unique_ptr<VectorRecord> record);

  auto getK() const -> int;
  auto getDim() const -> int;
  auto getRecord() -> std::unique_ptr<VectorRecord>;

 private:
  int k_ = 0;
  int dim_ = 0;
  std::unique_ptr<VectorRecord> record_;
};
};  // namespace candy
