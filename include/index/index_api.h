#pragma once

// Include the base index type definitions first
#include "index/index_types.h"
#include "index/index.h"

// Include all specific index implementations
#include "index/global_index.h"
#include "index/hnsw.h"
#include "index/ivf.h"
#include "index/knn.h"