#pragma once
#include <list>
#include <vector>
#include "runtime/operator/methods/join/base_method.h"
#include "runtime/function/join_function.h"

namespace candy {
class BruteForceLazy : public BaseMethod {
 public:
    void Excute(std :: vector<std :: pair<int, std::unique_ptr<VectorRecord>>> &emit_pool, std::unique_ptr<candy::JoinFunction> &joinfuc, std::list<std::unique_ptr<VectorRecord>> &left_records_, std::list<std::unique_ptr<VectorRecord>> &right_records_);
    void Excute(std :: vector<std :: pair<int, std::unique_ptr<VectorRecord>>> &emit_pool, std::unique_ptr<candy::JoinFunction> &joinfuc, std::unique_ptr<VectorRecord> &data, std::list<std::unique_ptr<VectorRecord>> &records_, int slot);
 private:
};
}  // namespace candy