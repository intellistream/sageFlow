#include <memory>

#include "concurrency/concurrency_controller.h"
#include "index/index.h"

namespace candy {
class BlankController final : public ConcurrencyController {
 public:
  BlankController();

  explicit BlankController(std::shared_ptr<Index> index);

  ~BlankController() override;

  auto insert(std::unique_ptr<VectorRecord> &record) -> bool override;

  auto erase(std::unique_ptr<VectorRecord> &record) -> bool override;

  auto query(std::unique_ptr<VectorRecord> &record, int k) -> std::vector<std::unique_ptr<VectorRecord>> override;

  auto erase(uint64_t uid) -> bool override;

 private:
  std::shared_ptr<Index> index_;
};
}  // namespace candy