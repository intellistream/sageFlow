# Data Persistence Framework

## Overview

The data persistence framework extends the data source framework to support saving generated test data to files and loading it back. This enables:

1. **Reproducibility** - Save generated datasets for consistent testing across runs
2. **Sharing** - Share test datasets between team members
3. **Debugging** - Inspect generated data in human-readable formats
4. **Performance** - Save once, reuse many times without regeneration overhead

## Architecture

### Components

```
Data Persistence Framework
├── Writers (Output)
│   ├── DataWriterBase        # Abstract writer interface
│   ├── FvecsWriter           # Binary format (.fvecs)
│   └── JsonWriter            # Human-readable format (.json)
├── Readers (Input - via DataSource)
│   ├── DatasetDataSource     # Reads .fvecs files
│   └── JsonDataSource        # Reads .json files
└── TestDataGenerator         # Enhanced with save/load support
```

### Supported Formats

#### 1. FVECS Format (.fvecs)
- **Type**: Binary format
- **Use Case**: Production, large datasets, efficiency
- **Format Spec**: `[dimension(int32)][vector_data(float32 * dimension)]` per vector
- **Pros**: Compact, fast I/O, industry standard
- **Cons**: Not human-readable

#### 2. JSON Format (.json)
- **Type**: Text format
- **Use Case**: Debugging, visualization, small datasets
- **Format Spec**: `{"dimension": N, "count": M, "vectors": [[...], [...]]}`
- **Pros**: Human-readable, easy to inspect, portable
- **Cons**: Larger file size, slower I/O

## Usage

### Basic Usage: Save Generated Data

```cpp
#include "test_utils/test_data_generator.h"
#include "test_utils/data_writer/fvecs_writer.h"
#include "test_utils/data_writer/json_writer.h"

// Generate test data
TestDataGenerator::Config config;
config.vector_dim = 128;
config.positive_pairs = 100;
config.negative_pairs = 100;
config.random_tail = 200;

TestDataGenerator generator(config);
auto [records, matches] = generator.generateData();

// Save to binary format (efficient for large datasets)
auto fvecs_writer = std::make_shared<FvecsWriter>();
generator.saveGeneratedVectors("test_data.fvecs", fvecs_writer);

// OR save to JSON format (human-readable for debugging)
auto json_writer = std::make_shared<JsonWriter>();
generator.saveGeneratedVectors("test_data.json", json_writer);
```

### Load and Use Saved Data

```cpp
#include "test_utils/test_data_generator.h"
#include "test_utils/data_source/dataset_data_source.h"
#include "test_utils/data_source/json_data_source.h"

// Load from FVECS file
DatasetDataSource::Config ds_config;
ds_config.file_path = "test_data.fvecs";
ds_config.expected_dim = 128;
ds_config.loop = true;  // Enable looping for reuse
auto data_source = std::make_shared<DatasetDataSource>(ds_config);

// OR load from JSON file
JsonDataSource::Config json_config;
json_config.file_path = "test_data.json";
json_config.loop = true;
auto json_source = std::make_shared<JsonDataSource>(json_config);

// Use loaded data with TestDataGenerator
TestDataGenerator::Config gen_config;
gen_config.similarity_threshold = 0.8;
gen_config.positive_pairs = 50;
gen_config.negative_pairs = 50;

TestDataGenerator generator(gen_config, data_source);
auto [records, matches] = generator.generateData();
```

### Workflow: Generate Once, Use Many Times

```cpp
// Step 1: Generate and save reference dataset (run once)
void generateReferenceDataset() {
    TestDataGenerator::Config config;
    config.vector_dim = 128;
    config.positive_pairs = 500;
    config.negative_pairs = 500;
    config.random_tail = 2000;
    config.seed = 42;  // Fixed seed for reproducibility
    
    TestDataGenerator generator(config);
    generator.generateData();
    
    auto writer = std::make_shared<FvecsWriter>();
    generator.saveGeneratedVectors("reference_dataset_v1.fvecs", writer);
}

// Step 2: Use in multiple tests without regeneration
TEST(MyTest, TestWithReferenceData) {
    DatasetDataSource::Config config;
    config.file_path = "reference_dataset_v1.fvecs";
    config.loop = true;
    auto data_source = std::make_shared<DatasetDataSource>(config);
    
    TestDataGenerator::Config gen_config;
    gen_config.positive_pairs = 100;
    TestDataGenerator generator(gen_config, data_source);
    
    auto [records, matches] = generator.generateData();
    // Run tests...
}
```

## File Format Specifications

### FVECS Format

Binary format, little-endian:
```
[int32: dimension] [float32: value_1] [float32: value_2] ... [float32: value_dim]
[int32: dimension] [float32: value_1] [float32: value_2] ... [float32: value_dim]
...
```

Example (dimension=3, 2 vectors):
```
Bytes:  [03 00 00 00] [3F 80 00 00] [40 00 00 00] [40 40 00 00]
Values: [3]            [1.0]          [2.0]          [3.0]
        
        [03 00 00 00] [40 80 00 00] [40 A0 00 00] [40 C0 00 00]
        [3]            [4.0]          [5.0]          [6.0]
```

### JSON Format

Text format with standard JSON syntax:
```json
{
  "dimension": 128,
  "count": 1000,
  "vectors": [
    [0.123456, -0.234567, 0.345678, ...],
    [0.456789, -0.567890, 0.678901, ...],
    ...
  ]
}
```

## Integration with Existing Tests

### Before (Direct Generation Only)
```cpp
TEST(MyTest, TestJoinOperator) {
    TestDataGenerator::Config config;
    config.vector_dim = 128;
    TestDataGenerator generator(config);
    auto [records, matches] = generator.generateData();
    // Test...
}
```

### After (Support Both Generation and Files)
```cpp
TEST(MyTest, TestJoinOperator) {
    // Option 1: Direct generation (same as before)
    TestDataGenerator::Config config;
    config.vector_dim = 128;
    TestDataGenerator generator(config);
    auto [records, matches] = generator.generateData();
    
    // Option 2: Load from file (NEW!)
    DatasetDataSource::Config ds_config;
    ds_config.file_path = "test_vectors.fvecs";
    auto data_source = std::make_shared<DatasetDataSource>(ds_config);
    TestDataGenerator generator2(config, data_source);
    auto [records2, matches2] = generator2.generateData();
    
    // Both work identically!
}
```

## API Reference

### DataWriterBase (Interface)

```cpp
class DataWriterBase {
  virtual bool writeVectors(const std::string& file_path, 
                           const std::vector<std::vector<float>>& vectors,
                           int dimension) = 0;
  virtual std::string getFileExtension() const = 0;
  virtual std::string getFormatDescription() const = 0;
};
```

### FvecsWriter

```cpp
class FvecsWriter : public DataWriterBase {
  // Writes vectors in FVECS binary format
  // file_path: Output file path (e.g., "data.fvecs")
  // Returns: true if successful, false otherwise
};
```

### JsonWriter

```cpp
class JsonWriter : public DataWriterBase {
  // Writes vectors in JSON text format
  // file_path: Output file path (e.g., "data.json")
  // Returns: true if successful, false otherwise
};
```

### JsonDataSource

```cpp
class JsonDataSource : public DataSourceBase {
  struct Config {
    std::string file_path;  // Path to JSON file
    bool loop = false;      // Loop back when reaching end
  };
  // Reads vectors from JSON files
};
```

### TestDataGenerator (Enhanced)

```cpp
class TestDataGenerator {
  // Save generated vectors to file
  bool saveGeneratedVectors(const std::string& file_path, 
                           std::shared_ptr<DataWriterBase> writer);
  
  // Get last generated vectors (for custom processing)
  std::vector<std::vector<float>> getLastGeneratedVectors() const;
};
```

## Best Practices

1. **Use FVECS for Production**: Binary format is efficient and industry-standard
2. **Use JSON for Debugging**: Human-readable format helps inspect data
3. **Version Your Datasets**: Include version in filename (e.g., `dataset_v1.fvecs`)
4. **Document Seed Values**: Always record the seed used to generate datasets
5. **Test Round-Trip**: Verify save/load preserves data integrity
6. **Enable Looping for Reuse**: Set `loop=true` when reusing datasets multiple times

## Examples

See `test/UnitTest/test_data_persistence.cpp` for comprehensive examples:
- Saving to different formats
- Round-trip testing (save and load back)
- Using loaded data with TestDataGenerator
- Format validation

## Performance Considerations

- **FVECS**: ~100-200 MB/s write speed, ~150-300 MB/s read speed
- **JSON**: ~20-50 MB/s write speed, ~30-80 MB/s read speed
- **Memory**: Writers process vectors in streaming fashion (low memory overhead)
- **Disk Space**: FVECS uses ~4 bytes/dimension/vector, JSON uses ~15-20 bytes/dimension/vector

## Backward Compatibility

All existing tests continue to work without modification. The persistence features are optional additions that don't affect the default behavior of TestDataGenerator or existing data sources.
