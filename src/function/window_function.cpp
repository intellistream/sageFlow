#include "function/window_function.h"
#include <spdlog/spdlog.h>
#include <algorithm>
#include <limits>
#include <numeric>

namespace candy {

DataElement AggregateFunction::process(const std::vector<DataElement>& elements,
                                  timestamp_t window_start,
                                  timestamp_t window_end) {
    if (elements.empty()) {
        spdlog::warn("AggregateFunction applied to empty window");
        return DataElement(); // Return empty element
    }
    
    switch (type_) {
        case AggregationType::COUNT: {
            // Simply return the count of records
            // We'll create a vector record with a single value representing the count
            uint64_t count = static_cast<uint64_t>(elements.size());
            
            // Create a record with count value
            auto result = std::make_unique<VectorRecord>(
                0,  // uid (could be window timestamp or other identifier)
                window_end,  // timestamp (use window end as the result timestamp)
                1,  // dimension
                DataType::Float64,  // using float64 for the count
                reinterpret_cast<char*>(&count)  // data pointer
            );
            
            return DataElement(std::move(result));
        }
            
        case AggregationType::SUM: {
            // Sum the values at the specified field index
            // For now, we'll assume we're summing vector values (e.g., first dimension)
            double sum = 0.0;
            
            for (const auto& element : elements) {
                if (element.isRecord() && element.getRecord()) {
                    const auto* record = element.getRecord();
                    // Get the value at the specified field index, assuming it's a vector
                    // This is a simplified example - real implementation would depend on data structure
                    if (record->data_.type_ == DataType::Float64 || 
                        record->data_.type_ == DataType::Float32) {
                        // Extract value from the specified dimension
                        if (field_index_ < record->data_.dim_) {
                            // For simplicity, assuming we can access Float64 data directly
                            // Real implementation would need proper type handling
                            if (record->data_.type_ == DataType::Float64) {
                                const double* values = reinterpret_cast<const double*>(record->data_.data_.get());
                                sum += values[field_index_];
                            } else if (record->data_.type_ == DataType::Float32) {
                                const float* values = reinterpret_cast<const float*>(record->data_.data_.get());
                                sum += static_cast<double>(values[field_index_]);
                            }
                        }
                    }
                }
            }
            
            // Create a result with the sum
            double sum_value = sum;
            auto result = std::make_unique<VectorRecord>(
                0,  // uid
                window_end,  // timestamp
                1,  // dimension
                DataType::Float64,  // type
                reinterpret_cast<char*>(&sum_value)  // data pointer
            );
            
            return DataElement(std::move(result));
        }
            
        case AggregationType::MIN: {
            // Find minimum value
            double min_val = std::numeric_limits<double>::max();
            
            for (const auto& element : elements) {
                if (element.isRecord() && element.getRecord()) {
                    const auto* record = element.getRecord();
                    // Get the value at the specified field index, assuming it's a vector
                    if (record->data_.type_ == DataType::Float64 || 
                        record->data_.type_ == DataType::Float32) {
                        if (field_index_ < record->data_.dim_) {
                            if (record->data_.type_ == DataType::Float64) {
                                const double* values = reinterpret_cast<const double*>(record->data_.data_.get());
                                min_val = std::min(min_val, values[field_index_]);
                            } else if (record->data_.type_ == DataType::Float32) {
                                const float* values = reinterpret_cast<const float*>(record->data_.data_.get());
                                min_val = std::min(min_val, static_cast<double>(values[field_index_]));
                            }
                        }
                    }
                }
            }
            
            // Create a result with the minimum value
            double min_value = min_val;
            auto result = std::make_unique<VectorRecord>(
                0,  // uid
                window_end,  // timestamp
                1,  // dimension
                DataType::Float64,  // type
                reinterpret_cast<char*>(&min_value)  // data pointer
            );
            
            return DataElement(std::move(result));
        }
            
        case AggregationType::MAX: {
            // Find maximum value
            double max_val = std::numeric_limits<double>::lowest();
            
            for (const auto& element : elements) {
                if (element.isRecord() && element.getRecord()) {
                    const auto* record = element.getRecord();
                    // Get the value at the specified field index, assuming it's a vector
                    if (record->data_.type_ == DataType::Float64 || 
                        record->data_.type_ == DataType::Float32) {
                        if (field_index_ < record->data_.dim_) {
                            if (record->data_.type_ == DataType::Float64) {
                                const double* values = reinterpret_cast<const double*>(record->data_.data_.get());
                                max_val = std::max(max_val, values[field_index_]);
                            } else if (record->data_.type_ == DataType::Float32) {
                                const float* values = reinterpret_cast<const float*>(record->data_.data_.get());
                                max_val = std::max(max_val, static_cast<double>(values[field_index_]));
                            }
                        }
                    }
                }
            }
            
            // Create a result with the maximum value
            double max_value = max_val;
            auto result = std::make_unique<VectorRecord>(
                0,  // uid
                window_end,  // timestamp
                1,  // dimension
                DataType::Float64,  // type
                reinterpret_cast<char*>(&max_value)  // data pointer
            );
            
            return DataElement(std::move(result));
        }
            
        case AggregationType::AVG: {
            // Calculate average value
            double sum = 0.0;
            size_t count = 0;
            
            for (const auto& element : elements) {
                if (element.isRecord() && element.getRecord()) {
                    const auto* record = element.getRecord();
                    // Get the value at the specified field index, assuming it's a vector
                    if (record->data_.type_ == DataType::Float64 || 
                        record->data_.type_ == DataType::Float32) {
                        if (field_index_ < record->data_.dim_) {
                            if (record->data_.type_ == DataType::Float64) {
                                const double* values = reinterpret_cast<const double*>(record->data_.data_.get());
                                sum += values[field_index_];
                                count++;
                            } else if (record->data_.type_ == DataType::Float32) {
                                const float* values = reinterpret_cast<const float*>(record->data_.data_.get());
                                sum += static_cast<double>(values[field_index_]);
                                count++;
                            }
                        }
                    }
                }
            }
            
            // Calculate average
            double avg = (count > 0) ? (sum / static_cast<double>(count)) : 0.0;
            
            // Create a result with the average value
            double avg_value = avg;
            auto result = std::make_unique<VectorRecord>(
                0,  // uid
                window_end,  // timestamp
                1,  // dimension
                DataType::Float64,  // type
                reinterpret_cast<char*>(&avg_value)  // data pointer
            );
            
            return DataElement(std::move(result));
        }
            
        default:
            spdlog::error("Unsupported aggregation type");
            return DataElement(); // Return empty element
    }
}

AggregateFunction::AggregateFunction(AggregateFunction::AggregationType type, int field_index)
	: type_(type), field_index_(field_index) {}

} // namespace candy