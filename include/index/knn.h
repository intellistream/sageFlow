#include "index/index.h"

namespace candy {
class Knn final : public Index {
 public:
  ~Knn() override;
  auto insert(uint64_t id) -> bool override;
  auto erase(uint64_t id) -> bool override;
  auto query(std::unique_ptr<VectorRecord>& record, int k) -> std::vector<uint64_t> override;
  auto query_for_join(std::unique_ptr<VectorRecord> &record,
                          double join_similarity_threshold) -> std::vector<uint64_t> override {
    // NOT IMPLEMENTED;
    return {};
  }
};
}  // namespace candy
