#include "index/index.h"

namespace candy {
class VectraFlow final : public Index {
    private:
        std::vector<uint64_t> datas;
 public:
  ~VectraFlow() override;
  auto insert(uint64_t id) -> bool override;
  auto erase(uint64_t id) -> bool override;
  auto query(std::unique_ptr<VectorRecord>& record, int k) -> std::vector<int32_t> override;
};
}  // namespace candy
