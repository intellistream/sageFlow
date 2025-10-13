// Example: Data Persistence - Generate, Save, and Load Test Data
//
// This example demonstrates:
// 1. Generating test data with TestDataGenerator
// 2. Saving to multiple formats (FVECS and JSON)
// 3. Loading saved data back
// 4. Using loaded data with TestDataGenerator

#include <iostream>
#include "test_utils/test_data_generator.h"
#include "test_utils/data_source/random_data_source.h"
#include "test_utils/data_source/dataset_data_source.h"
#include "test_utils/data_source/json_data_source.h"
#include "test_utils/data_writer/fvecs_writer.h"
#include "test_utils/data_writer/json_writer.h"
#include "test_utils/test_data_adapter.h"

using namespace sageFlow::test;

void example_save_generated_data() {
    std::cout << "\n=== Example 1: Generate and Save Data ===" << std::endl;
    
    // Configure generator
    TestDataGenerator::Config config;
    config.vector_dim = 64;
    config.positive_pairs = 50;
    config.negative_pairs = 50;
    config.random_tail = 100;
    config.seed = 42;
    
    // Generate test data
    TestDataGenerator generator(config);
    std::cout << "Generating test data..." << std::endl;
    auto [records, matches] = generator.generateData();
    
    std::cout << "Generated " << records.size() << " records" << std::endl;
    std::cout << "Expected matches: " << matches.size() << std::endl;
    
    // Save to FVECS format (binary, efficient)
    std::string fvecs_path = "/tmp/test_data.fvecs";
    auto fvecs_writer = std::make_shared<FvecsWriter>();
    if (generator.saveGeneratedVectors(fvecs_path, fvecs_writer)) {
        std::cout << "✓ Saved to FVECS: " << fvecs_path << std::endl;
    }
    
    // Save to JSON format (human-readable)
    std::string json_path = "/tmp/test_data.json";
    auto json_writer = std::make_shared<JsonWriter>();
    if (generator.saveGeneratedVectors(json_path, json_writer)) {
        std::cout << "✓ Saved to JSON: " << json_path << std::endl;
    }
}

void example_load_from_fvecs() {
    std::cout << "\n=== Example 2: Load from FVECS File ===" << std::endl;
    
    // Load data from FVECS file
    DatasetDataSource::Config config;
    config.file_path = "/tmp/test_data.fvecs";
    config.expected_dim = 64;
    config.loop = false;
    
    try {
        auto data_source = std::make_shared<DatasetDataSource>(config);
        
        std::cout << "Dimension: " << data_source->getDimension() << std::endl;
        std::cout << "Total vectors: " << data_source->getTotalCount() << std::endl;
        
        // Read first 3 vectors
        int count = 0;
        while (data_source->hasMore() && count < 3) {
            auto vec = data_source->getNextVector();
            std::cout << "Vector " << count << " (first 5 components): ";
            for (int i = 0; i < 5; ++i) {
                std::cout << vec[i] << " ";
            }
            std::cout << std::endl;
            count++;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}

void example_load_from_json() {
    std::cout << "\n=== Example 3: Load from JSON File ===" << std::endl;
    
    // Load data from JSON file
    JsonDataSource::Config config;
    config.file_path = "/tmp/test_data.json";
    config.loop = false;
    
    try {
        auto data_source = std::make_shared<JsonDataSource>(config);
        
        std::cout << "Dimension: " << data_source->getDimension() << std::endl;
        std::cout << "Total vectors: " << data_source->getTotalCount() << std::endl;
        
        // Read first 3 vectors
        int count = 0;
        while (data_source->hasMore() && count < 3) {
            auto vec = data_source->getNextVector();
            std::cout << "Vector " << count << " (first 5 components): ";
            for (int i = 0; i < 5; ++i) {
                std::cout << vec[i] << " ";
            }
            std::cout << std::endl;
            count++;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}

void example_reuse_saved_data() {
    std::cout << "\n=== Example 4: Reuse Saved Data with TestDataGenerator ===" << std::endl;
    
    // Load previously saved data
    DatasetDataSource::Config ds_config;
    ds_config.file_path = "/tmp/test_data.fvecs";
    ds_config.expected_dim = 64;
    ds_config.loop = true;  // Enable looping for reuse
    
    try {
        auto data_source = std::make_shared<DatasetDataSource>(ds_config);
        
        // Use loaded data to generate new test dataset
        TestDataGenerator::Config gen_config;
        gen_config.similarity_threshold = 0.8;
        gen_config.positive_pairs = 20;
        gen_config.near_threshold_pairs = 0;
        gen_config.negative_pairs = 20;
        gen_config.random_tail = 40;
        
        TestDataGenerator generator(gen_config, data_source);
        auto [records, matches] = generator.generateData();
        
        std::cout << "Generated " << records.size() << " records from loaded data" << std::endl;
        std::cout << "Expected matches: " << matches.size() << std::endl;
        
        // Show first few records
        int count = 0;
        for (const auto& record : records) {
            if (count >= 5) break;
            auto vec = extractFloatVector(*record);
            std::cout << "Record " << count 
                      << " - UID: " << record->uid_ 
                      << ", TS: " << record->timestamp_
                      << ", Dim: " << vec.size() << std::endl;
            count++;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}

void example_workflow() {
    std::cout << "\n=== Example 5: Complete Workflow ===" << std::endl;
    
    // Step 1: Generate reference dataset
    std::cout << "\nStep 1: Generate reference dataset" << std::endl;
    TestDataGenerator::Config ref_config;
    ref_config.vector_dim = 128;
    ref_config.positive_pairs = 100;
    ref_config.negative_pairs = 100;
    ref_config.random_tail = 200;
    ref_config.seed = 12345;  // Fixed seed for reproducibility
    
    TestDataGenerator ref_generator(ref_config);
    ref_generator.generateData();
    
    std::string ref_path = "/tmp/reference_dataset_v1.fvecs";
    auto writer = std::make_shared<FvecsWriter>();
    ref_generator.saveGeneratedVectors(ref_path, writer);
    std::cout << "✓ Saved reference dataset: " << ref_path << std::endl;
    
    // Step 2: Use reference dataset in test
    std::cout << "\nStep 2: Load and use reference dataset" << std::endl;
    DatasetDataSource::Config load_config;
    load_config.file_path = ref_path;
    load_config.expected_dim = 128;
    load_config.loop = true;
    
    auto ref_source = std::make_shared<DatasetDataSource>(load_config);
    
    TestDataGenerator::Config test_config;
    test_config.positive_pairs = 50;
    test_config.near_threshold_pairs = 0;
    test_config.negative_pairs = 50;
    test_config.random_tail = 100;
    
    TestDataGenerator test_generator(test_config, ref_source);
    auto [test_records, test_matches] = test_generator.generateData();
    
    std::cout << "✓ Generated " << test_records.size() 
              << " test records from reference dataset" << std::endl;
    
    // Step 3: Save test-specific variant
    std::cout << "\nStep 3: Save test-specific variant for debugging" << std::endl;
    std::string debug_path = "/tmp/test_variant_debug.json";
    auto json_writer = std::make_shared<JsonWriter>();
    test_generator.saveGeneratedVectors(debug_path, json_writer);
    std::cout << "✓ Saved debug variant: " << debug_path << std::endl;
    std::cout << "  (You can inspect this JSON file to debug test failures)" << std::endl;
}

int main() {
    std::cout << "Data Persistence Examples" << std::endl;
    std::cout << "=========================" << std::endl;
    
    example_save_generated_data();
    example_load_from_fvecs();
    example_load_from_json();
    example_reuse_saved_data();
    example_workflow();
    
    std::cout << "\n✓ All examples completed!" << std::endl;
    std::cout << "\nGenerated files in /tmp:" << std::endl;
    std::cout << "  - test_data.fvecs (binary format)" << std::endl;
    std::cout << "  - test_data.json (human-readable format)" << std::endl;
    std::cout << "  - reference_dataset_v1.fvecs (reference dataset)" << std::endl;
    std::cout << "  - test_variant_debug.json (debug variant)" << std::endl;
    
    return 0;
}
