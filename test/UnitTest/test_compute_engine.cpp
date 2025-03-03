#include <core/common/data_types.h>
#include <core/compute_engine/compute_engine.h>
#include <gtest/gtest.h>

#include <cmath>
#include <memory>

TEST(SimilarityTest, CalculateSimilarity) {
  auto vec1 = std::make_unique<candy::VectorRecord>("id1", candy::VectorData(std::vector<float>{1.0, 2.0, 3.0}), 1);
  auto vec2 = std::make_unique<candy::VectorRecord>("id2", candy::VectorData(std::vector<float>{4.0, 5.0, 6.0}), 2);

  double similarity = candy::ComputeEngine::calculateSimilarity(vec1, vec2);
  EXPECT_NEAR(similarity, 0.974631846, 0.0001);
}

TEST(DistanceTest, ComputeEuclideanDistance) {
  auto vec1 = std::make_unique<candy::VectorRecord>("id1", candy::VectorData(std::vector<float>{1.0, 2.0, 3.0}), 1);
  auto vec2 = std::make_unique<candy::VectorRecord>("id2", candy::VectorData(std::vector<float>{4.0, 5.0, 6.0}), 2);

  double distance = candy::ComputeEngine::computeEuclideanDistance(vec1, vec2);
  EXPECT_NEAR(distance, 5.196152422, 0.0001);
}

TEST(NormalizeTest, NormalizeVector) {
  auto vec = std::make_unique<candy::VectorRecord>("id1", candy::VectorData(std::vector<float>{3.0, 4.0}), 1);
  auto normalized = candy::ComputeEngine::normalizeVector(vec);

  EXPECT_EQ(normalized->data_->size(), vec->data_->size());
  EXPECT_NEAR((*normalized->data_)[0], 0.6, 0.0001);
  EXPECT_NEAR((*normalized->data_)[1], 0.8, 0.0001);
}

TEST(TopKTest, FindTopK) {
  std::vector<std::unique_ptr<candy::VectorRecord>> records;
  records.push_back(std::make_unique<candy::VectorRecord>("id1", candy::VectorData(std::vector<float>{1.0, 2.0}), 1));
  records.push_back(std::make_unique<candy::VectorRecord>("id2", candy::VectorData(std::vector<float>{3.0, 4.0}), 2));
  records.push_back(std::make_unique<candy::VectorRecord>("id3", candy::VectorData(std::vector<float>{5.0, 6.0}), 3));
 

  auto top_k =
      candy::ComputeEngine::findTopK(records, 2, [](std::unique_ptr<candy::VectorRecord>& record) {
    return record->timestamp_; });

  EXPECT_EQ(top_k.size(), 2);
  EXPECT_EQ(top_k[0]->id_, "id3");
  EXPECT_EQ(top_k[1]->id_, "id2");
}
