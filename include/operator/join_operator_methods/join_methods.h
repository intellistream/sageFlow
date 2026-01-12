#pragma once

#include "operator/join_operator_methods/base_method.h"
#include "operator/join_operator_methods/bruteforce_baseline.h"
#include "operator/join_operator_methods/ivf_method.h"
#include "operator/join_operator_methods/hnsw.h"
#include "operator/join_operator_methods/hdr_tree_method.h"
#include "operator/join_operator_methods/clustered_join_method.h"
// Note: s3j_method.h and vsjoin_method.h are intentionally not included here
// due to conflicting PartitionStats definitions. Include them separately if needed.