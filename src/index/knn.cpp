//
// Created by Pygon on 25-4-17.
//
#include "index/knn.h"
#include <iostream>

candy::Knn::~Knn() = default;

auto candy::Knn::insert(uint64_t id) -> bool { return true; }

auto candy::Knn::erase(uint64_t id) -> bool { return true; }

auto candy::Knn::query(std::unique_ptr<VectorRecord>& record, int k) -> std::vector<uint64_t> {
  //return {};
  //std :: cerr << storage_manager_->map_.size() << std :: endl;
  auto idxes = storage_manager_->topk(record, k);
  return idxes;
}