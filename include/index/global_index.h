#pragma once

#include <string>
#include "index/index.h"

namespace candy {

/**
 * @brief Global index implementation with persistence capabilities
 * 
 * GlobalIndex extends the base Index class with functionality for
 * saving and loading index structures to/from persistent storage.
 */
class GlobalIndex final : public Index {
 public:
  // Default constructor
  GlobalIndex() = default;
  
  // Destructor
  ~GlobalIndex() override = default;
  
  // Implement abstract methods from Index base class
  auto insert(uint64_t id) -> bool override;
  auto erase(uint64_t id) -> bool override;
  auto query(std::unique_ptr<VectorRecord> &record, int k) -> std::vector<uint64_t> override;
  
  // Persistence operations specific to GlobalIndex
  auto save(const std::string &path) -> bool;
  auto load(const std::string &path) -> bool;
  auto remove() -> bool;
};

}  // namespace candy