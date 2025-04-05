// vector_database.cpp
#include <vector_db/vector_database.h>
#include <cstdint>

namespace candy {

VectorDatabase::VectorDatabase(const std::string &storage_path)
    : storage_engine_(std::make_shared<StorageEngine>(storage_path)),
      anns_(std::make_shared<ANNS>()),
      query_executor_(std::make_shared<QueryExecutor>(storage_engine_, anns_)) {}

void VectorDatabase::addVector(const std::shared_ptr<VectorRecord> &record) const { query_executor_->addVector(record); }

void VectorDatabase::removeVector(const uint64_t &id) const { query_executor_->removeVector(id); }

auto VectorDatabase::executeKNN(const VectorData &query, const size_t k) const
    -> std::vector<std::shared_ptr<VectorRecord>> {
  return query_executor_->executeKNN(query, k);
}

}  // namespace candy