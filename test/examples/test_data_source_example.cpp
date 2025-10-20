// Example: Using DatasetDataSource to generate test data from real datasets
//
// This example demonstrates how to use the data source framework to:
// 1. Load vectors from a dataset file
// 2. Generate test data with dataset vectors
// 3. Use the data with join operators

#include <iostream>
#include "test_utils/test_data_generator.h"
#include "test_utils/data_source/random_data_source.h"
#include "test_utils/data_source/dataset_data_source.h"
#include "test_utils/test_data_adapter.h"

using namespace sageFlow::test;

void example_random_data_source() {
    std::cout << "\n=== Example 1: Random Data Source ===" << std::endl;
    
    // Configure random data source
    RandomDataSource::Config ds_config;
    ds_config.vector_dim = 64;
    ds_config.seed = 42;
    ds_config.max_vectors = 100;
    
    auto data_source = std::make_shared<RandomDataSource>(ds_config);
    
    std::cout << "Dimension: " << data_source->getDimension() << std::endl;
    std::cout << "Total vectors: " << data_source->getTotalCount() << std::endl;
    
    // Get first 5 vectors
    int count = 0;
    while (data_source->hasMore() && count < 5) {
        auto vec = data_source->getNextVector();
        std::cout << "Vector " << count << " (first 5 components): ";
        for (int i = 0; i < 5; ++i) {
            std::cout << vec[i] << " ";
        }
        std::cout << std::endl;
        count++;
    }
}

void example_dataset_data_source() {
    std::cout << "\n=== Example 2: Dataset Data Source ===" << std::endl;
    
    // Configure dataset data source
    DatasetDataSource::Config ds_config;
    ds_config.file_path = PROJECT_DIR "/data/siftsmall/siftsmall_query.fvecs";
    ds_config.expected_dim = 128;
    ds_config.loop = false;
    
    try {
        auto data_source = std::make_shared<DatasetDataSource>(ds_config);
        
        std::cout << "Dimension: " << data_source->getDimension() << std::endl;
        std::cout << "Total vectors: " << data_source->getTotalCount() << std::endl;
        
        // Get first 3 vectors
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

void example_test_data_generator_with_dataset() {
    std::cout << "\n=== Example 3: TestDataGenerator with Dataset ===" << std::endl;
    
    // Create dataset data source
    DatasetDataSource::Config ds_config;
    ds_config.file_path = PROJECT_DIR "/data/siftsmall/siftsmall_query.fvecs";
    ds_config.expected_dim = 128;
    ds_config.loop = true;  // Enable looping to allow reuse
    
    try {
        auto data_source = std::make_shared<DatasetDataSource>(ds_config);
        
        // Configure test data generator
        TestDataGenerator::Config config;
        config.positive_pairs = 5;
        config.negative_pairs = 5;
        config.near_threshold_pairs = 2;
        config.random_tail = 10;
        config.similarity_threshold = 0.8;
        
        // Create generator with dataset source
        TestDataGenerator generator(config, data_source);
        
        // Generate test data
        auto [records, expected_matches] = generator.generateData();
        
        std::cout << "Generated " << records.size() << " records" << std::endl;
        std::cout << "Expected matches: " << expected_matches.size() << std::endl;
        
        // Show first few records
        int count = 0;
        for (const auto& record : records) {
            if (count >= 5) break;
            auto vec = extractFloatVector(*record);
            std::cout << "Record " << count << " - UID: " << record->uid_ 
                      << ", TS: " << record->timestamp_
                      << ", Dim: " << vec.size() << std::endl;
            count++;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}

int main() {
    std::cout << "Data Source Framework Examples" << std::endl;
    std::cout << "===============================" << std::endl;
    
    example_random_data_source();
    example_dataset_data_source();
    example_test_data_generator_with_dataset();
    
    std::cout << "\nAll examples completed!" << std::endl;
    return 0;
}
