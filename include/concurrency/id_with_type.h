#pragma once

#include "index/index_types.h"

namespace candy {

/**
 * @brief Struct that associates an ID with an index type
 * 
 * Used by the concurrency manager to track which indices correspond to which types
 */
struct IdWithType {
  int id_;               // The index ID
  IndexType index_type_; // The type of index
};

}  // namespace candy