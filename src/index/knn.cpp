//
// Created by Pygon on 25-4-17.
//
#include "index/knn.h"
#include <iostream>

candy::Knn::~Knn() = default;

auto candy::Knn::insert(uint64_t id) -> bool { return true; }

auto candy::Knn::erase(uint64_t id) -> bool { return true; }

auto candy::Knn::query(const VectorRecord &record, int k) -> std::vector<uint64_t> {
  auto idxes = storage_manager_->topk(record, k);
  return idxes;
}

auto candy::Knn::query_for_join(const VectorRecord &record,
                    double join_similarity_threshold) -> std::vector<uint64_t> {
  return storage_manager_->similarityJoinQuery(record, join_similarity_threshold);
}