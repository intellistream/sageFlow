# Join Data Source Framework

## Overview

The Join Data Source Framework provides a flexible, modular architecture for generating test data for join operations. It extracts the data generation logic from individual test files into reusable components that support multiple data source strategies.

## Problem Statement

Previously, join tests followed a common pattern:
1. Generate data with `TestDataGenerator`
2. Duplicate the data to create left and right streams
3. Apply UID offsets to distinguish streams
4. Feed to join operators

This pattern was repeated across multiple test files with slight variations, making it:
- **Repetitive** - Same logic duplicated in many places
- **Inflexible** - Hard to test with different data distributions
- **Coupled** - Data generation tied to test implementation

## Solution

The Join Data Source Framework provides:
- **`JoinDataSourcePair`** - Manages creation of left/right streams from various sources
- **`JoinDataSourceConfig`** - Configuration for different streaming strategies
- **`JoinTestHelper`** - Convenience functions for common patterns
- **`JoinDataSourceFactory`** - Factory methods for standard configurations

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│            Join Test (test_join_*.cpp)                    │
└────────────────────────┬─────────────────────────────────┘
                         │
         ┌───────────────┴───────────────┐
         │                               │
    ┌────▼──────────┐           ┌───────▼────────┐
    │ JoinTestHelper│           │JoinDataSourcePair│
    └────┬──────────┘           └───────┬────────┘
         │                               │
         └───────────────┬───────────────┘
                         │
              ┌──────────▼──────────┐
              │ JoinDataSourceConfig│
              └──────────┬──────────┘
                         │
       ┌─────────────────┼─────────────────┐
       │                 │                 │
  ┌────▼────┐     ┌──────▼──────┐   ┌─────▼─────┐
  │Duplicate│     │  Separate   │   │ Generated │
  │  Mode   │     │   Mode      │   │   Mode    │
  └─────────┘     └─────────────┘   └───────────┘
```

## Usage

### Mode 1: Generated (Backward Compatible)

This is the default mode that maintains compatibility with existing tests:

```cpp
#include "test_utils/join_test_helper.h"
#include "test_utils/test_data_generator.h"

// Generate data as before
TestDataGenerator::Config config;
config.vector_dim = 128;
config.positive_pairs = 50;
config.negative_pairs = 100;

TestDataGenerator generator(config);

// NEW: Use helper to create join streams
auto [left_records, right_records] = 
    JoinTestHelper::generateJoinStreamsFromGenerator(generator);

// Use with join operator (same as before)
for (auto& rec : left_records) {
    // Process left stream
}
for (auto& rec : right_records) {
    // Process right stream
}
```

### Mode 2: Duplicate from Single Source

Test with a specific dataset or pattern by duplicating one source:

```cpp
#include "test_utils/join_test_helper.h"
#include "test_utils/data_source/dataset_data_source.h"

// Load a dataset
DatasetDataSource::Config ds_config;
ds_config.file_path = "data/siftsmall/siftsmall_query.fvecs";
ds_config.expected_dim = 128;
auto source = std::make_shared<DatasetDataSource>(ds_config);

// Generate join streams from dataset
auto [left_records, right_records] = 
    JoinTestHelper::generateJoinStreamsFromSource(source);

// Both streams contain same data (from dataset)
// Right stream UIDs are offset by default
```

### Mode 3: Separate Sources

Test join with different data distributions on each side:

```cpp
#include "test_utils/join_test_helper.h"
#include "test_utils/data_source/random_data_source.h"

// Create different sources
RandomDataSource::Config left_config;
left_config.vector_dim = 128;
left_config.seed = 111;  // Different seed
auto left_source = std::make_shared<RandomDataSource>(left_config);

RandomDataSource::Config right_config;
right_config.vector_dim = 128;
right_config.seed = 222;  // Different seed
auto right_source = std::make_shared<RandomDataSource>(right_config);

// Generate from separate sources
auto [left_records, right_records] = 
    JoinTestHelper::generateJoinStreamsFromSeparateSources(
        left_source, right_source);

// Left and right streams have different data distributions
```

### Mode 4: Advanced Configuration

For fine-grained control, use `JoinDataSourcePair` directly:

```cpp
#include "test_utils/join_data_source.h"

// Create custom configuration
JoinDataSourceConfig config;
config.mode = JoinDataSourceConfig::Mode::Duplicate;
config.single_source = my_source;
config.apply_right_uid_offset = false;  // No UID offset
config.right_uid_offset = 1000000;      // Custom offset if enabled
config.base_timestamp = 2000000;        // Custom timestamps
config.time_interval = 50;              // Custom intervals

// Create pair and generate
JoinDataSourcePair pair(config);
auto [left, right] = pair.generateStreams(100);  // Limit to 100 records
```

## API Reference

### JoinTestHelper

Helper functions for common patterns:

```cpp
class JoinTestHelper {
  // Generate from TestDataGenerator (backward compatible)
  static pair<vector<VectorRecord>, vector<VectorRecord>>
  generateJoinStreamsFromGenerator(
      TestDataGenerator& generator,
      bool apply_uid_offset = true);

  // Generate from single source (duplicate mode)
  static pair<vector<VectorRecord>, vector<VectorRecord>>
  generateJoinStreamsFromSource(
      shared_ptr<DataSourceBase> source,
      bool apply_uid_offset = true,
      size_t max_records = 0);

  // Generate from separate sources
  static pair<vector<VectorRecord>, vector<VectorRecord>>
  generateJoinStreamsFromSeparateSources(
      shared_ptr<DataSourceBase> left_source,
      shared_ptr<DataSourceBase> right_source,
      bool apply_uid_offset = false,
      size_t max_records = 0);
};
```

### JoinDataSourceFactory

Factory methods for standard configurations:

```cpp
class JoinDataSourceFactory {
  // Create duplicate mode config
  static JoinDataSourceConfig createDuplicated(
      shared_ptr<DataSourceBase> source,
      bool apply_uid_offset = true);

  // Create separate mode config
  static JoinDataSourceConfig createSeparate(
      shared_ptr<DataSourceBase> left_source,
      shared_ptr<DataSourceBase> right_source,
      bool apply_uid_offset = false);

  // Create generated mode config (backward compatible)
  static JoinDataSourceConfig createGenerated(
      shared_ptr<DataSourceBase> source,
      bool apply_uid_offset = true);
};
```

### JoinDataSourcePair

Main class for generating join streams:

```cpp
class JoinDataSourcePair {
  explicit JoinDataSourcePair(const JoinDataSourceConfig& config);

  // Generate left and right streams
  pair<vector<VectorRecord>, vector<VectorRecord>>
  generateStreams(size_t max_records = 0);

  // Get dimension
  int getDimension() const;

  // Get total count
  int getTotalCount() const;

  // Reset to beginning
  void reset();
};
```

## Configuration Options

### JoinDataSourceConfig

```cpp
struct JoinDataSourceConfig {
  enum class Mode {
    Duplicate,    // Same source duplicated to both sides
    Separate,     // Different sources for left/right
    Generated     // TestDataGenerator mode (backward compatible)
  };

  Mode mode = Mode::Generated;

  // For Duplicate/Generated mode
  shared_ptr<DataSourceBase> single_source;

  // For Separate mode
  shared_ptr<DataSourceBase> left_source;
  shared_ptr<DataSourceBase> right_source;

  // Common options
  bool apply_right_uid_offset = true;   // Offset right UIDs
  uint64_t right_uid_offset = 500000;   // Default offset value
  int64_t base_timestamp = 1000000;     // Starting timestamp
  int64_t time_interval = 100;          // Time increment
};
```

## Migration Guide

### Updating Existing Tests

**Before:**
```cpp
TestDataGenerator generator(config);
auto [records, _] = generator.generateData();

// Manual duplication
std::vector<std::unique_ptr<VectorRecord>> left_records;
for (auto& r : records) {
    left_records.push_back(std::make_unique<VectorRecord>(*r));
}

std::vector<std::unique_ptr<VectorRecord>> right_records;
constexpr uint64_t kOffset = 500000;
for (auto& r : records) {
    right_records.push_back(std::make_unique<VectorRecord>(
        r->uid_ + kOffset, r->timestamp_, r->data_));
}
```

**After:**
```cpp
TestDataGenerator generator(config);
auto [left_records, right_records] = 
    JoinTestHelper::generateJoinStreamsFromGenerator(generator);
```

### Adding New Test Scenarios

**Test with real dataset:**
```cpp
TEST_F(MyJoinTest, WithSIFTDataset) {
    DatasetDataSource::Config config;
    config.file_path = "data/siftsmall/siftsmall_query.fvecs";
    config.expected_dim = 128;
    auto source = std::make_shared<DatasetDataSource>(config);

    auto [left, right] = 
        JoinTestHelper::generateJoinStreamsFromSource(source, true, 50);

    // Test join with real vectors
    testJoinOperation(left, right);
}
```

**Test with asymmetric distributions:**
```cpp
TEST_F(MyJoinTest, AsymmetricDistributions) {
    // Dense left, sparse right
    auto left_source = createDenseVectorSource();
    auto right_source = createSparseVectorSource();

    auto [left, right] = 
        JoinTestHelper::generateJoinStreamsFromSeparateSources(
            left_source, right_source);

    // Test how join handles different distributions
    testJoinOperation(left, right);
}
```

## Benefits

1. **Code Reuse** - Eliminate duplication across test files
2. **Flexibility** - Easy to test with various data sources
3. **Maintainability** - Change data generation strategy in one place
4. **Testability** - Test framework itself is unit tested
5. **Backward Compatible** - Existing tests work unchanged
6. **Extensible** - Easy to add new modes or configurations

## Testing

Comprehensive tests in `test/UnitTest/test_join_data_source.cpp`:

```bash
# Run join data source tests
./bin/test_join_data_source

# All 8 test cases pass:
# - DuplicateMode
# - SeparateMode  
# - HelperWithTestDataGenerator
# - HelperWithSingleSource
# - HelperWithSeparateSources
# - WithDatasetSource
# - MaxRecordsLimit
# - ResetFunctionality
```

## Examples

See `test/UnitTest/test_join_data_source.cpp` for comprehensive examples of all usage patterns.

## Future Enhancements

Potential additions:
- **Streaming mode** - Generate data on-demand instead of all at once
- **Time-based patterns** - Configure complex timestamp patterns
- **UID strategies** - Pluggable UID generation strategies
- **Batch support** - Generate multiple batches with reset
- **Statistics** - Track generation statistics for analysis
