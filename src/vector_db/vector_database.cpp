// vector_database.cpp
#include <vector_db/vector_database.h>

namespace candy {

VectorDatabase::VectorDatabase(const std::string &storagePath)
    : storageEngine(std::make_shared<StorageEngine>(storagePath)),
      anns(std::make_shared<ANNS>()),
      queryExecutor(std::make_shared<QueryExecutor>(storageEngine, anns)) {}

void VectorDatabase::addVector(const std::shared_ptr<VectorRecord> &record) const { queryExecutor->addVector(record); }

void VectorDatabase::removeVector(const std::string &id) const { queryExecutor->removeVector(id); }

auto VectorDatabase::executeKNN(const VectorData &query, const size_t k) const
    -> std::vector<std::shared_ptr<VectorRecord>> {
  return queryExecutor->executeKNN(query, k);
}

}  // namespace candy