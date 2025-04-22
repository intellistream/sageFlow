#include "compute_engine/compute_engine.h"

auto candy::ComputeEngine::calculateSimilarity(const VectorData& vec1, const VectorData& vec2) -> double {
    return {};
}

auto candy::ComputeEngine::computeEuclideanDistance(const VectorData& vec1, const VectorData& vec2) -> double {
    return {};
}

auto candy::ComputeEngine::normalizeVector(const VectorData& vec) -> VectorData {
    return VectorData{1,Float32};
}

candy::ComputeEngine::ComputeEngine() {}