#include <list>
#include <vector>

#include "operator/join_operator_methods/base_method.h"
#include "function/join_function.h"

namespace candy {
class BruteForceEager : public BaseMethod {
 public:
  explicit BruteForceEager(double join_similarity_threshold) : BaseMethod(join_similarity_threshold) {}
  ~BruteForceEager() override = default;
  void Excute(std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
              std::unique_ptr<JoinFunction> &joinfuc,
              std::list<std::unique_ptr<VectorRecord>> &left_records,
              std::list<std::unique_ptr<VectorRecord>> &right_records) override;

  void Excute(std::vector<std::pair<int, std::unique_ptr<VectorRecord>>> &emit_pool,
              std::unique_ptr<JoinFunction> &joinfuc,
              std::unique_ptr<VectorRecord> &data,
              std::list<std::unique_ptr<VectorRecord>> &records,
              int slot) override;
 private:
};
}  // namespace candy