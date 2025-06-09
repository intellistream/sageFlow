#pragma once
#include <list>
#include <vector>

#include "function/join_function.h"

namespace candy {

enum class JoinMethodType {
  BRUTEFORCE_EAGER,
  BRUTEFORCE_LAZY,
  IVF_EAGER,
  IVF_LAZY
};

class BaseMethod {
 public:
   virtual ~BaseMethod() = default;
   virtual void Excute(std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
                       std::unique_ptr<JoinFunction> &joinfuc,
                       std::list<std::unique_ptr<VectorRecord>> &left_records,
                       std::list<std::unique_ptr<VectorRecord>> &right_records);

   virtual void Excute(std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
                       std::unique_ptr<JoinFunction> &joinfuc,
                       std::unique_ptr<VectorRecord> &data,
                       std::list<std::unique_ptr<VectorRecord>> &records,
                       int slot);
 private:
};
}  // namespace candy