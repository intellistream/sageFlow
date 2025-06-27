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
  explicit BaseMethod(double join_similarity_threshold)
    : join_similarity_threshold_(join_similarity_threshold){}

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
 protected:
  double join_similarity_threshold_;
 private:
};
}  // namespace candy