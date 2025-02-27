


#include <core/common/data_types.h>
#include <vector_db/indexing/anns.h>
#include <vector_db/storage_engine.h>
#include <memory>
#include <string>
#include <vector>

namespace candy {

class QueryExecutor {
public:
  QueryExecutor(std::shared_ptr<StorageEngine> storage,
                std::shared_ptr<ANNS> anns);

  // Execute a k-NN search query
  std::vector<std::shared_ptr<VectorRecord>> executeKNN(const VectorData &query,
                                                        size_t k) const;

  // Add a vector
  void addVector(const std::shared_ptr<VectorRecord> &record);

  // Remove a vector
  void removeVector(const std::string &id);

private:
  std::shared_ptr<StorageEngine> storageEngine;
  std::shared_ptr<ANNS> anns;
};

} // namespace candy


