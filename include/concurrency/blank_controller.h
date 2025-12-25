#include <memory>

#include "concurrency/concurrency_controller.h"
#include "index/index.h"

namespace sageFlow {
class BlankController final : public ConcurrencyController {
 public:
  BlankController();

  explicit BlankController(std::shared_ptr<Index> index);

  ~BlankController() override;

  auto insert(std::unique_ptr<VectorRecord> record) -> bool override;

  auto erase(std::unique_ptr<VectorRecord> record) -> bool override;

  auto query(const VectorRecord& record, int k) -> std::vector<std::shared_ptr<const VectorRecord>> override;

  auto query_for_join(const VectorRecord& record,
                      double join_similarity_threshold) -> std::vector<std::shared_ptr<const VectorRecord>> override;

  auto erase(uint64_t uid) -> bool override;

  /**
   * @brief 获取底层索引（用于分区索引访问）
   * @return Index 共享指针
   */
  auto getIndex() const -> std::shared_ptr<Index> { return index_; }

 private:
  std::shared_ptr<Index> index_;
};
}  // namespace sageFlow