#include "test_utils/data_source/skewed_data_source.h"
#include <cmath>
#include <numeric>
#include <iostream>

namespace sageFlow { namespace test {

SkewedDataSource::SkewedDataSource(const Config& config) 
    : config_(config), rng_(config.seed) {
    initCentroids();
    initDistribution();
}

void SkewedDataSource::initCentroids() {
    centroids_.reserve(config_.num_clusters);
    for(int i=0; i<config_.num_clusters; ++i) {
        centroids_.push_back(generateRandomVector());
    }
}

void SkewedDataSource::initDistribution() {
    std::vector<double> weights(config_.num_clusters);
    for(int i=0; i<config_.num_clusters; ++i) {
        // Zipf: 1 / (rank ^ s)
        // rank starts at 1
        weights[i] = 1.0 / std::pow(i + 1, config_.zipf_skew);
    }
    cluster_dist_ = std::discrete_distribution<int>(weights.begin(), weights.end());
}

std::vector<float> SkewedDataSource::generateRandomVector() {
    std::vector<float> vec(config_.vector_dim);
    std::normal_distribution<float> dist(0.0f, 1.0f);
    float norm = 0.0f;
    for(int i=0; i<config_.vector_dim; ++i) {
        vec[i] = dist(rng_);
        norm += vec[i] * vec[i];
    }
    // Normalize
    norm = std::sqrt(norm);
    if(norm > 1e-6) {
        for(int i=0; i<config_.vector_dim; ++i) vec[i] /= norm;
    }
    return vec;
}

std::vector<float> SkewedDataSource::getNextVector() {
    int cluster_idx = cluster_dist_(rng_);
    last_cluster_index_ = cluster_idx;
    
    // Generate vector near centroid
    const auto& centroid = centroids_[cluster_idx];
    std::vector<float> vec(config_.vector_dim);
    std::normal_distribution<float> noise_dist(0.0f, config_.cluster_spread);
    
    float norm = 0.0f;
    for(int i=0; i<config_.vector_dim; ++i) {
        vec[i] = centroid[i] + noise_dist(rng_);
        norm += vec[i] * vec[i];
    }
    
    // Normalize again
    norm = std::sqrt(norm);
    if(norm > 1e-6) {
        for(int i=0; i<config_.vector_dim; ++i) vec[i] /= norm;
    }
    
    generated_count_++;
    return vec;
}

bool SkewedDataSource::hasMore() const {
    if(config_.max_vectors < 0) return true;
    return generated_count_ < config_.max_vectors;
}

void SkewedDataSource::reset() {
    rng_.seed(config_.seed);
    generated_count_ = 0;
    // Keep centroids to ensure same distribution, just reset stream
}

}} // namespace
