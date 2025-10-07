//
// Created by Pygon on 25-4-17.
//
#include "index/knn.h"
#include <iostream>

sageFlow::Knn::~Knn() = default;

auto sageFlow::Knn::insert(uint64_t id) -> bool { return true; }

auto sageFlow::Knn::erase(uint64_t id) -> bool { return true; }

auto sageFlow::Knn::query(const VectorRecord &record, int k) -> std::vector<uint64_t> {
  auto idxes = storage_manager_->topk(record, k);
  return idxes;
}

auto sageFlow::Knn::query_for_join(const VectorRecord &record,
                    double join_similarity_threshold) -> std::vector<uint64_t> {
  return storage_manager_->similarityJoinQuery(record, join_similarity_threshold);
}