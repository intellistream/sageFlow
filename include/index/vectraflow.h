#include "index/index.h"

namespace candy {
class VectraFlow final : public Index {
 private:
  std::vector<uint64_t> datas_;

 public:
  ~VectraFlow() override;
  auto insert(uint64_t id) -> bool override;
  auto erase(uint64_t id) -> bool override;
  auto query(const VectorRecord &record, int k) -> std::vector<uint64_t> override;
  auto query_for_join(const VectorRecord &record,
                          double join_similarity_threshold) -> std::vector<uint64_t> override {
    // NOT IMPLEMENTED;
    return {};
  }
};
}  // namespace candy
