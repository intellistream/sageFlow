#include "index/index.h"

#include <shared_mutex>
#include <unordered_set>
#include <vector>

namespace sageFlow {
// BruteForce 索引。与 IVF 一样，索引自身负责其内部数据结构（成员 UID 集合）的
// 并发安全：BlankController 的锁只保护 index_ 指针的原子替换，不保护索引内部数据。
// 这里维护本索引拥有的 UID 集合，使查询只扫描属于该 index 的成员，而不是 StorageManager 全量。
class Knn final : public Index {
 public:
  ~Knn() override;
  auto insert(uint64_t id) -> bool override;
  auto erase(uint64_t id) -> bool override;
  auto query(const VectorRecord &record, int k) -> std::vector<uint64_t> override;
  auto query_for_join(const VectorRecord &record,
                      double join_similarity_threshold,
                      double similarity_alpha) -> std::vector<uint64_t> override;

 private:
  auto snapshotIds() const -> std::vector<uint64_t>;

  mutable std::shared_mutex ids_mutex_;
  std::unordered_set<uint64_t> live_ids_;
};
}  // namespace sageFlow
