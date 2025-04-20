#pragma once
#include <functional>
#include <vector>

#include "common/data_types.h"
#include "function/function.h"

namespace candy {
using TopkFunc = std::function<std::vector<VectorRecord>(std::unique_ptr<VectorRecord> &)>;

class TopkFunction final : public Function {
 public:
  explicit TopkFunction(const std::string &name);

  TopkFunction(const std::string &name, const TopkFunc &topk_func);

  auto Execute(Response &resp) -> Response override;

 private:
  TopkFunc topk_func_;
};
};  // namespace candy