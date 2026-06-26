#pragma once

#include <memory>

#include "concurrency/concurrency_controller.h"
#include "index/index.h"

namespace sageFlow {

/// Lock-free concurrency controller for partition-local indexes.
/// Designed for single-writer / single-reader scenarios where
/// external synchronization is guaranteed by the partition topology.
class DirectController final : public ConcurrencyController {
 public:
  DirectController();
  explicit DirectController(std::shared_ptr<Index> index);
  ~DirectController() override;

  auto insert(std::unique_ptr<VectorRecord> record) -> bool override;
  auto insert(RecordView record) -> bool override;
  auto erase(std::unique_ptr<VectorRecord> record) -> bool override;
  auto erase(uint64_t uid) -> bool override;
  auto query(const VectorRecord& record, int k) -> std::vector<std::shared_ptr<const VectorRecord>> override;
  auto query_for_join(const VectorRecord& record,
                      double join_similarity_threshold,
                      double similarity_alpha) -> std::vector<std::shared_ptr<const VectorRecord>> override;

  auto getIndex() const -> std::shared_ptr<Index> override;
  auto replaceIndex(std::shared_ptr<Index> new_index) -> bool override;

 private:
  std::shared_ptr<Index> index_{nullptr};
};

}  // namespace sageFlow
