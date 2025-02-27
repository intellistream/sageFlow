


#include <vector_db/storage_engine.h>
#include <vector_db/indexing/anns.h>
#include <vector_db/query_executor.h>
#include <memory>

namespace candy {

class VectorDatabase {
public:
  VectorDatabase(const std::string& storagePath);

  // Add a vector
  void addVector(const std::shared_ptr<VectorRecord>& record) const;

  // Remove a vector
  void removeVector(const std::string& id) const;

  // Execute a k-NN query
  std::vector<std::shared_ptr<VectorRecord>> executeKNN(const VectorData& query, size_t k) const;

private:
  std::shared_ptr<StorageEngine> storageEngine;
  std::shared_ptr<ANNS> anns;
  std::shared_ptr<QueryExecutor> queryExecutor;
};

} // namespace candy


