# Test Data Source Framework

## Overview

The test data source framework provides a modular and extensible way to generate test data for the sageFlow operators. The framework separates data generation logic from test code, making it easier to:

- Use different data sources (random, dataset-based, etc.)
- Reuse data generation logic across tests
- Test with real-world datasets
- Maintain backward compatibility with existing tests

## Architecture

The framework consists of several data source implementations:

### 1. DataSourceBase (Abstract Base Class)

Located in: `test/test_utils/data_source/data_source_base.h`

Defines the interface for all data sources:
- `getNextVector()`: Returns the next vector from the source
- `getDimension()`: Returns the dimension of vectors
- `hasMore()`: Checks if more data is available
- `reset()`: Resets the source to start from beginning
- `getTotalCount()`: Returns total number of vectors (if known)

### 2. RandomDataSource

Located in: `test/test_utils/data_source/random_data_source.h/cpp`

Generates random normalized vectors using a configurable seed for reproducibility.

**Configuration:**
```cpp
RandomDataSource::Config config;
config.vector_dim = 128;      // Vector dimension
config.seed = 42;             // Random seed for reproducibility
config.max_vectors = -1;      // Max vectors to generate (-1 = unlimited)

auto data_source = std::make_shared<RandomDataSource>(config);
```

### 3. DatasetDataSource

Located in: `test/test_utils/data_source/dataset_data_source.h/cpp`

Loads vectors from fvecs format dataset files (commonly used in vector search benchmarks).

**Configuration:**
```cpp
DatasetDataSource::Config config;
config.file_path = "data/siftsmall/siftsmall_query.fvecs";
config.expected_dim = 128;    // Expected dimension (-1 = auto-detect)
config.loop = true;           // Loop back to start when reaching end

auto data_source = std::make_shared<DatasetDataSource>(ds_config);
```

### 4. VectorListSource

Located in: `test/test_utils/data_source/vector_list_source.h`

A simple adapter that wraps an in-memory vector of float vectors. Useful for:
- Wrapping generated data from TestDataGenerator
- Testing with small, predefined datasets
- Creating data sources from computed vectors

**Usage:**
```cpp
#include "test_utils/data_source/vector_list_source.h"

// Create from a vector of vectors
std::vector<std::vector<float>> vectors = {
    {0.1f, 0.2f, 0.3f},
    {0.4f, 0.5f, 0.6f},
    {0.7f, 0.8f, 0.9f}
};

auto data_source = std::make_shared<VectorListSource>(vectors);

// Use like any other data source
while (data_source->hasMore()) {
    auto vec = data_source->getNextVector();
    // Process vector
}
```

**Note:** This is primarily an internal utility used by JoinTestHelper to wrap TestDataGenerator output, but can be used directly if needed.

### 5. JsonDataSource

Located in: `test/test_utils/data_source/json_data_source.h/cpp`

Loads vectors from JSON format files. Useful for debugging and human-readable datasets.

**Configuration:**
```cpp
JsonDataSource::Config config;
config.file_path = "test_data.json";
config.expected_dim = 128;  // Optional validation
config.loop = false;         // Whether to loop when reaching end

auto data_source = std::make_shared<JsonDataSource>(config);
```

## Usage

### Using with TestDataGenerator

The `TestDataGenerator` class has been updated to accept a custom data source:

#### Option 1: Default Random Generation (Backward Compatible)
```cpp
TestDataGenerator::Config config;
config.vector_dim = 128;
config.positive_pairs = 100;
config.negative_pairs = 100;

// Uses random data source internally
TestDataGenerator generator(config);
auto [records, expected_matches] = generator.generateData();
```

#### Option 2: Custom Random Data Source
```cpp
// Create a custom random data source
RandomDataSource::Config ds_config;
ds_config.vector_dim = 64;
ds_config.seed = 123;
auto data_source = std::make_shared<RandomDataSource>(ds_config);

// Use with TestDataGenerator
TestDataGenerator::Config config;
config.similarity_threshold = 0.8;
config.positive_pairs = 50;

TestDataGenerator generator(config, data_source);
auto [records, expected_matches] = generator.generateData();
```

#### Option 3: Dataset-Based Generation
```cpp
// Load vectors from a dataset file
DatasetDataSource::Config ds_config;
ds_config.file_path = PROJECT_DIR "/data/siftsmall/siftsmall_query.fvecs";
ds_config.expected_dim = 128;
ds_config.loop = true;  // Enable looping for reuse
auto data_source = std::make_shared<DatasetDataSource>(ds_config);

// Generate test data using dataset vectors
TestDataGenerator::Config config;
config.similarity_threshold = 0.8;
config.positive_pairs = 10;
config.negative_pairs = 10;

TestDataGenerator generator(config, data_source);
auto [records, expected_matches] = generator.generateData();
```

### Direct Use of Data Sources

Data sources can also be used directly without TestDataGenerator:

```cpp
// Create a data source
RandomDataSource::Config config;
config.vector_dim = 128;
config.seed = 42;
auto data_source = std::make_shared<RandomDataSource>(config);

// Get vectors directly
while (data_source->hasMore()) {
    std::vector<float> vec = data_source->getNextVector();
    // Use the vector...
}

// Reset to start again
data_source->reset();
```

## Available Datasets

The repository includes the SIFT small dataset in `data/siftsmall/`:
- `siftsmall_base.fvecs` - Base vectors (10,000 vectors, 128D)
- `siftsmall_query.fvecs` - Query vectors (100 vectors, 128D)
- `siftsmall_learn.fvecs` - Learning vectors (25,000 vectors, 128D)

## Extending the Framework

To add a new data source:

1. Create a new class inheriting from `DataSourceBase`
2. Implement all virtual methods
3. Add the new source files to `test/CMakeLists.txt`
4. Use it with `TestDataGenerator` or directly in tests

Example skeleton:

```cpp
class MyCustomDataSource : public DataSourceBase {
public:
    struct Config {
        // Your configuration options
    };

    explicit MyCustomDataSource(const Config& config);

    std::vector<float> getNextVector() override;
    int getDimension() const override;
    bool hasMore() const override;
    void reset() override;
    int getTotalCount() const override;

private:
    Config config_;
    // Your implementation details
};
```

## Backward Compatibility

All existing tests continue to work without modification. The default constructor of `TestDataGenerator` automatically creates a `RandomDataSource` internally, maintaining the original behavior.

## Testing

See `test/UnitTest/test_data_source.cpp` for comprehensive examples of using the data source framework.

Run the data source tests:
```bash
cd build
./bin/test_data_source
```

Or run all unit tests:
```bash
cd build
ctest -L UNIT
```
