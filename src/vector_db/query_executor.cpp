#include <vector_db/query_executor.h>

namespace candy {

QueryExecutor::QueryExecutor(std::shared_ptr<StorageEngine> storage, std::shared_ptr<ANNS> anns)
    : storage_engine_(std::move(storage)), anns_(std::move(anns)) {}

auto QueryExecutor::executeKNN(const VectorData& query, size_t k) const -> std::vector<std::shared_ptr<VectorRecord>> {
  return anns_->search(query, k);
}

void QueryExecutor::addVector(const std::shared_ptr<VectorRecord>& record) {
  storage_engine_->add(record);
  anns_->insert(record);
}

void QueryExecutor::removeVector(const uint64_t& id) {
  storage_engine_->remove(id);
  anns_->remove(id);
}

}  // namespace candy