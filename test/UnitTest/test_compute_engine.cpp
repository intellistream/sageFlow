#include <common/data_types.h>
#include <compute_engine/compute_engine.h>
#include <gtest/gtest.h>

#include <cmath>
#include <memory>

std::unique_ptr<sageFlow::VectorData> makeVec(const std::vector<float>& v) {
  auto d = std::make_unique<sageFlow::VectorData>(v.size(), sageFlow::DataType::Float32);
  std::memcpy(d->data_.get(), v.data(), v.size()*sizeof(float));
  return d;
}

TEST(SimilarityTest, Identical) {
  sageFlow::ComputeEngine eng;
  auto a = makeVec({1,2,3}), b = makeVec({1,2,3});
  double sim = eng.Similarity(*a,*b, 0.5);
  EXPECT_DOUBLE_EQ(sim, 1.0);
}

TEST(SimilarityTest, KnownDist) {
  sageFlow::ComputeEngine eng;
  auto a = makeVec({0,0}), b = makeVec({3,4});
  double alpha = 0.1;
  double expected = std::exp(-alpha * 5.0); // 距离为 5
  EXPECT_DOUBLE_EQ(eng.Similarity(*a,*b, alpha), expected);
}

TEST(SimilarityTest, ZeroDim) {
  sageFlow::ComputeEngine eng;
  auto a = makeVec({}), b = makeVec({});
  EXPECT_DOUBLE_EQ(eng.Similarity(*a,*b,1.0), 1.0);
}

TEST(SimilarityTest, GreaterSimilarity) {
  sageFlow::ComputeEngine eng;
  auto a = makeVec({0,0,0}), b = makeVec({1,1,1});
  auto c = makeVec({2,2,2});
  EXPECT_GT(eng.Similarity(*a,*b,1.0), eng.Similarity(*a,*c,1.0));
}

TEST(DistanceTest, ComputeEuclideanDistance) {
  sageFlow::ComputeEngine eng;
  auto a = makeVec({0,0}), b = makeVec({3,4});
  EXPECT_DOUBLE_EQ(eng.EuclideanDistance(*a,*b), 5.0);
}

TEST(NormalizeTest, NormalizeVector) {


}
