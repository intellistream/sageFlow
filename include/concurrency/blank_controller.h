#include <memory>
#include <shared_mutex>

#include "concurrency/concurrency_controller.h"
#include "index/index.h"

namespace sageFlow {
class BlankController final : public ConcurrencyController {
 public:
  BlankController();

  explicit BlankController(std::shared_ptr<Index> index);

  ~BlankController() override;

  auto insert(std::unique_ptr<VectorRecord> record) -> bool override;
  auto insert(RecordView record) -> bool override;

  auto erase(std::unique_ptr<VectorRecord> record) -> bool override;

  auto query(const VectorRecord& record, int k) -> std::vector<std::shared_ptr<const VectorRecord>> override;

  auto query_for_join(const VectorRecord& record,
                      double join_similarity_threshold,
                      double similarity_alpha) -> std::vector<std::shared_ptr<const VectorRecord>> override;

  auto erase(uint64_t uid) -> bool override;

  auto getIndex() const -> std::shared_ptr<Index> override;
  auto replaceIndex(std::shared_ptr<Index> new_index) -> bool override;
  auto enableDoubleWrite(bool enable, std::shared_ptr<Index> shadow = nullptr) -> void override;

 private:
  mutable std::shared_mutex index_mutex_;
  std::shared_ptr<Index> index_{nullptr};
  std::shared_ptr<Index> shadow_index_{nullptr};
  bool double_write_enabled_{false};
};
}  // namespace sageFlow
