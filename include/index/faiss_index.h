#pragma once

#include <memory>
#include <string>
#include <vector>
#include <shared_mutex>

#include "index/index.h"

namespace faiss {
struct Index;
}

namespace sageFlow {

class FaissIndex : public Index {
 public:
  /**
   * @brief FaissIndex 构造函数
   * @param dimension 向量维度
   * @param index_description Faiss 索引工厂字符串 
   * @param metric_type 度量类型 
   */
  FaissIndex(int dimension, const std::string& index_description, int metric_type = 0);

  ~FaissIndex() override;

  auto insert(uint64_t id) -> bool override;
  auto erase(uint64_t id) -> bool override;
  auto query(const VectorRecord &record, int k) -> std::vector<uint64_t> override;
  auto query_for_join(const VectorRecord &record,
                      double join_similarity_threshold) -> std::vector<uint64_t> override;

  // 设置搜索参数的辅助函数 
  void setParameter(const std::string& name, double value);

 private:
  std::unique_ptr<faiss::Index> faiss_index_;
  std::vector<float> training_buffer_;
  std::vector<int64_t> training_ids_;
  size_t training_threshold_ = 100;
  
  mutable std::shared_mutex mutex_;
};

}  // namespace sageFlow
