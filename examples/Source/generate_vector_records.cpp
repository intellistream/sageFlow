#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include <chrono>

#include "common/data_types.h"

// Helper function to generate a random vector
candy::VectorData generateRandomVectorData(std::mt19937& gen, int dim, candy::DataType dataType) {
    candy::VectorData vectorData(dim, dataType);
    
    // Create distributions based on data type
    std::uniform_int_distribution<int> int_dist(-100, 100);
    std::uniform_real_distribution<float> float_dist(-100.0f, 100.0f);
    std::uniform_real_distribution<double> double_dist(-100.0, 100.0);
    
    // Fill the vector with random data based on its type
    int element_size = candy::DATA_TYPE_SIZE[dataType];
    for (int i = 0; i < dim; ++i) {
        switch (dataType) {
            case candy::Int8: {
                int8_t value = static_cast<int8_t>(int_dist(gen) % 128);
                std::memcpy(vectorData.data_.get() + i * element_size, &value, element_size);
                break;
            }
            case candy::Int16: {
                int16_t value = static_cast<int16_t>(int_dist(gen));
                std::memcpy(vectorData.data_.get() + i * element_size, &value, element_size);
                break;
            }
            case candy::Int32: {
                int32_t value = int_dist(gen);
                std::memcpy(vectorData.data_.get() + i * element_size, &value, element_size);
                break;
            }
            case candy::Int64: {
                int64_t value = static_cast<int64_t>(int_dist(gen));
                std::memcpy(vectorData.data_.get() + i * element_size, &value, element_size);
                break;
            }
            case candy::Float32: {
                float value = float_dist(gen);
                std::memcpy(vectorData.data_.get() + i * element_size, &value, element_size);
                break;
            }
            case candy::Float64: {
                double value = double_dist(gen);
                std::memcpy(vectorData.data_.get() + i * element_size, &value, element_size);
                break;
            }
            default:
                break;
        }
    }
    
    return vectorData;
}

int main(int argc, char* argv[]) {
    // Set the output file path
    std::string output_path = "vector_records.bin";
    if (argc > 1) {
        output_path = argv[1];
    }
    
    // Number of records to generate
    int num_records = 1000;
    if (argc > 2) {
        num_records = std::stoi(argv[2]);
    }
    
    // Open output file
    std::ofstream output_file(output_path, std::ios::binary);
    if (!output_file.is_open()) {
        std::cerr << "Failed to open output file: " << output_path << std::endl;
        return 1;
    }
    
    // Set up random number generator
    std::random_device rd;
    std::mt19937 gen(rd());
    
    // Distributions for various fields
    std::uniform_int_distribution<uint64_t> uid_dist(1, 1000000);
    std::uniform_int_distribution<int> dim_dist(2, 128);  // More reasonable dimensions
    std::uniform_int_distribution<int> type_dist(1, 6);    // Data types from Int8 to Float64
    
    // Current timestamp as starting point - use high-precision clock
    auto now = std::chrono::high_resolution_clock::now();
    int64_t base_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    
    std::cout << "Generating " << num_records << " vector records..." << std::endl;
    
    // Write number of records as header
    int32_t record_count = num_records;
    output_file.write(reinterpret_cast<char*>(&record_count), sizeof(int32_t));
    
    for (int i = 0; i < num_records; ++i) {
        // Generate random values for the vector record
        uint64_t uid = uid_dist(gen);
        int64_t timestamp = base_timestamp + i*10;  // Sequential timestamps with 10ms interval
        
        // Fixed dimension and type for more consistent data
        int32_t dim = 4;  // Fixed dimension for simplicity
        candy::DataType type = candy::DataType::Float32;  // Fixed type for simplicity
        
        // Generate random vector data
        candy::VectorData vector_data = generateRandomVectorData(gen, dim, type);
        
        // Create vector record
        candy::VectorRecord record(uid, timestamp, std::move(vector_data));
        
        // Serialize and write to file
        if (!record.Serialize(output_file)) {
            std::cerr << "Failed to serialize record " << i << std::endl;
            output_file.close();
            return 1;
        }
        
        // Progress output
        if (i % 100 == 0 || i == num_records - 1) {
            std::cout << "Generated " << (i+1) << " records..." << std::endl;
        }
    }
    
    output_file.close();
    std::cout << "Successfully generated " << num_records << " vector records to " << output_path << std::endl;
    std::cout << "Base timestamp: " << base_timestamp << std::endl;
    
    return 0;
}
