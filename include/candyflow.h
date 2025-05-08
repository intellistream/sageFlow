#pragma once

/**
 * @file candyflow.h
 * @brief Main API header for the candyFlow vector streaming library
 *
 * This header provides access to all major components of the candyFlow system,
 * allowing users to easily include the necessary functionality without
 * having to include individual component headers.
 */

// Common data types and utilities
#include "common/data_types.h"
#include "utils/conf_map.h"
#include "utils/error_codes.h"
#include "utils/monitoring.h"

// Core processing components
#include "compute_engine/compute_engine.h"
#include "storage/storage_api.h"
#include "concurrency/concurrency_api.h"

// Stream processing
#include "stream/stream_api.h"
#include "function/function_api.h"
#include "operator/operator_api.h"

// Indexing components
#include "index/index_api.h"

// Query optimization
#include "query/optimizer/planner.h"

namespace candy {

/**
 * @brief Library version information
 */
struct Version {
    static constexpr int MAJOR = 0;
    static constexpr int MINOR = 1;
    static constexpr int PATCH = 0;
    
    static constexpr const char* STRING = "0.1.0";
};

}  // namespace candy