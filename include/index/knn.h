#include "index/index.h"

namespace candy {
class Knn final : public Index {
 public:
  ~Knn() override;
  auto insert(uint64_t id) -> bool override;
  auto erase(uint64_t id) -> bool override;
  auto query(std::unique_ptr<VectorRecord>& record, int k) -> std::vector<int32_t> override;
};
}  // namespace candy
