# Remaining Work for Data Source Integration

## Summary of Completed Work

1. ✅ **Logging Replacement** - All std::cout/std::cerr replaced with SAGEFLOW_LOG
2. ✅ **Data Source Configuration** - Added DataSourceFactory and TestDataGenerator::createFromConfig()
3. ✅ **Example Configuration** - Created config/perf_join_with_datasource.toml

## Remaining Work

### Add New Test Cases to test_join_perf_scaling.cpp

The user requested: "除了实现其他的comment提到的问题以外，在关键的测试文件中（如test_join_perf_scaling），仿照目前使用generator的模式，使用random和sift数据再写同样的测试用例"

**What Needs to Be Done:**
1. Add test cases in test_join_perf_scaling.cpp that demonstrate using:
   - Random data source (similar to current, but explicitly using RandomDataSource)
   - SIFT dataset from data/siftsmall/siftsmall_query.fvecs

**Implementation Approach:**

```cpp
// In test_join_perf_scaling.cpp, add new parameterized test cases:

// Example 1: Test with random data source (explicit)
TEST_P(JoinPerformanceTest, PerfTestWithRandomDataSource) {
  auto [method, size, parallelism, win_ms] = GetParam();
  
  // Create random data source explicitly
  RandomDataSource::Config ds_config;
  ds_config.vector_dim = 64;
  ds_config.seed = 42;
  ds_config.max_vectors = size;
  auto data_source = std::make_shared<RandomDataSource>(ds_config);
  
  // Create generator with explicit data source
  TestDataGenerator::Config gen_config;
  gen_config.vector_dim = 64;
  gen_config.positive_pairs = size / 4;
  gen_config.similarity_threshold = 0.8;
  TestDataGenerator generator(gen_config, data_source);
  
  // Rest of test follows existing pattern...
  auto [left, right] = JoinTestHelper::generateJoinStreamsFromGenerator(generator, true);
  // ... execute join test
}

// Example 2: Test with SIFT dataset
TEST_P(JoinPerformanceTest, PerfTestWithSiftDataset) {
  auto [method, size, parallelism, win_ms] = GetParam();
  
  // Check if SIFT data is available
  std::string sift_path = PROJECT_DIR "/data/siftsmall/siftsmall_query.fvecs";
  if (!std::filesystem::exists(sift_path)) {
    GTEST_SKIP() << "SIFT dataset not found at " << sift_path;
  }
  
  // Create dataset data source
  DatasetDataSource::Config ds_config;
  ds_config.file_path = sift_path;
  ds_config.expected_dim = 128;  // SIFT dimension
  ds_config.loop = true;  // Allow reuse if dataset smaller than needed
  auto data_source = std::make_shared<DatasetDataSource>(ds_config);
  
  // Create generator with dataset source
  TestDataGenerator::Config gen_config;
  gen_config.vector_dim = 128;  // SIFT dimension
  gen_config.positive_pairs = std::min(size / 4, 100);  // Limit for dataset size
  gen_config.similarity_threshold = 0.8;
  TestDataGenerator generator(gen_config, data_source);
  
  // Rest of test follows existing pattern...
  auto [left, right] = JoinTestHelper::generateJoinStreamsFromGenerator(generator, true);
  // ... execute join test
}
```

**Key Considerations:**
1. The new tests should follow the same pattern as existing performance tests
2. SIFT test should check file existence and skip gracefully if not found
3. Should use the same JoinTestHelper interface for consistency
4. Tests should produce comparable performance metrics
5. May need to adjust test parameters (sizes, pairs) for dataset constraints

**Files to Modify:**
- test/Performance/test_join_perf_scaling.cpp - Add new test cases

**Expected Outcome:**
- Demonstration of framework flexibility with different data sources
- Performance comparison between random and real-world data
- Backward compatibility maintained (existing tests unchanged)
- All tests pass successfully
