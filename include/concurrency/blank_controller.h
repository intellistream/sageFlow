#include <memory>

#include "concurrency/concurrency_controller.h"
#include "index/index.h"

namespace candy {
class BlankController : public ConcurrencyController {
 public:
  BlankController() = default;

  ~BlankController() override = default;

  auto insert(std::unique_ptr<VectorRecord> &record) -> bool override {
    auto idx = storage_manager_->insert(record);
    if (index_) {
      index_->insert(idx);
    }
    return true;
  }

  auto erase(std::unique_ptr<VectorRecord> &record) -> bool override { return true; }

  auto query(std::unique_ptr<VectorRecord> &record, int k) -> std::vector<std::unique_ptr<VectorRecord>> override {
    auto idxes = index_->query(record, k);
    return storage_manager_->getVectorsByIds(idxes);
  }

 private:
  std::shared_ptr<Index> index_;
};
}  // namespace candy