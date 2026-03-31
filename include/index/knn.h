#include "index/index.h"
#include <shared_mutex>
#include <unordered_map>
#include <memory>

namespace sageFlow {
class Knn final : public Index {
 public:
  ~Knn() override;
  auto insert(uint64_t id) -> bool override;
  auto erase(uint64_t id) -> bool override;
  auto query(const VectorRecord &record, int k) -> std::vector<uint64_t> override;
  auto query_for_join(const VectorRecord &record,
                      double join_similarity_threshold,
                      double similarity_alpha) -> std::vector<uint64_t> override;

  /// Number of records currently in this index
  size_t size() const;

 private:
  mutable std::shared_mutex local_mutex_;
  // Local record cache: uid -> VectorRecord copy (owned)
  std::unordered_map<uint64_t, std::shared_ptr<const VectorRecord>> local_records_;
};
}  // namespace sageFlow
