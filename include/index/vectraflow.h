#include "index/index.h"

namespace sageFlow {
class VectraFlow final : public Index {
 private:
  std::vector<uint64_t> datas_;

 public:
  ~VectraFlow() override;
  auto insert(uint64_t id) -> bool override;
  auto erase(uint64_t id) -> bool override;
  auto query(const VectorRecord &record, int k) -> std::vector<uint64_t> override;
  auto query_for_join(const VectorRecord &record,
                          double join_similarity_threshold,
                          double similarity_alpha) -> std::vector<uint64_t> override {
    // NOT IMPLEMENTED;
    (void)join_similarity_threshold;
    (void)similarity_alpha;
    return {};
  }
};
}  // namespace sageFlow
