#include <iostream>
#include <memory>
#include <string>
#include <spdlog/spdlog.h>

#include "function/filter_function.h"
#include "function/map_function.h"
#include "function/window_function.h"
#include "function/sink_function.h"
#include "stream/data_stream_source/file_stream_source.h"
#include "stream/stream_environment.h"

using namespace candy;

// Instead of custom classes, define functions using lambdas or standard functions

int main(int argc, char* argv[]) {
    try {
        // Check if file path was provided
        if (argc < 2) {
            std::cerr << "Usage: " << argv[0] << " <vector_file_path>" << std::endl;
            return 1;
        }

        std::string file_path = argv[1];
        
        // Set up logging
        spdlog::set_level(spdlog::level::debug);
        spdlog::info("Starting windowed streaming example");
        
        // Create the stream environment
        spdlog::debug("Creating StreamEnvironment");
        StreamEnvironment env;
        spdlog::debug("StreamEnvironment created");
        
        // Set the global allowed lateness to 2 seconds
        spdlog::debug("Setting global allowed lateness");
        env.setAllowedLateness(2000); // 2000 ms
        spdlog::debug("Global allowed lateness set to 2000ms");
        
        // Create a file stream source
        spdlog::debug("Creating FileStreamSource with file: {}", file_path);
        auto file_source = std::make_shared<FileStreamSource>("vector_source", file_path);
        spdlog::debug("FileStreamSource created successfully");
        
        spdlog::debug("Setting watermark interval");
        file_source->setWatermarkInterval(500); // Generate watermarks every 500ms
        spdlog::debug("Setting watermark delay");
        file_source->setWatermarkDelay(1000);   // 1 second behind max event time
        spdlog::debug("Watermark properties set");
        
        // Connect the source to the stream environment
        spdlog::debug("Setting stream environment to file source");
        file_source->setStreamEnvironment(env);
        spdlog::debug("Stream environment set to file source");
        
        // Create the data processing pipeline with windowing using lambda functions
        
        spdlog::debug("Creating filter function");
        // Create a filter function using FilterFunc (std::function)
        FilterFunc highValueFilterFunc = [](std::unique_ptr<VectorRecord>& record) -> bool {
            spdlog::debug("Filter function called");
            // Filter records with first dimension value > 0.5
            if (!record) {
                spdlog::warn("Filter received null record");
                return false;
            }
            
            if (record->data_.type_ != DataType::Float64) {
                spdlog::warn("Filter received non-Float64 record type: {}", static_cast<int>(record->data_.type_));
                return false;
            }
            
            const double* values = reinterpret_cast<const double*>(record->data_.data_.get());
            if (!values) {
                spdlog::warn("Filter received record with null data pointer");
                return false;
            }
            
            spdlog::debug("Filter checking value: {}", values[0]);
            return values[0] > 0.5;
        };
        spdlog::debug("Filter function created");
        
        spdlog::debug("Creating map function");
        // Create a map function using MapFunc (std::function)
        MapFunc timestampMapperFunc = [](std::unique_ptr<VectorRecord>& record) -> std::unique_ptr<VectorRecord> {
            spdlog::debug("Map function called");
            // Example: Add processing timestamp to the record
            if (!record) {
                spdlog::warn("Map received null record");
                return nullptr;
            }
            
            // In a real example, we might add additional fields or transform the data
            // Here we'll just log the event time and pass the record through
            spdlog::debug("Processing record with timestamp: {}", record->timestamp_);
            return std::move(record);
        };
        spdlog::debug("Map function created");
        
        spdlog::debug("Creating sink function");
        // Create a sink function using SinkFunc (std::function)
        SinkFunc windowResultSinkFunc = [](std::unique_ptr<VectorRecord>& record) -> void {
            spdlog::debug("Sink function called");
            if (!record) {
                spdlog::warn("Sink received null record");
                return;
            }
            
            if (record->data_.type_ != DataType::Float64) {
                spdlog::warn("Sink received non-Float64 record type: {}", static_cast<int>(record->data_.type_));
                return;
            }
            
            const double* values = reinterpret_cast<const double*>(record->data_.data_.get());
            if (!values) {
                spdlog::warn("Sink received record with null data pointer");
                return;
            }
            
            spdlog::info("Window result: timestamp={}, value={}", record->timestamp_, values[0]);
        };
        spdlog::debug("Sink function created");
        
        // Use the function wrappers with the stream operations
        spdlog::debug("Creating filter wrapper");
        auto filter_func = std::make_unique<FilterFunction>("high_value_filter", highValueFilterFunc);
        if (!filter_func) {
            spdlog::critical("Failed to create filter function wrapper");
            return 1;
        }
        
        spdlog::debug("Applying filter to file source");
        auto filtered_stream = file_source->filter(std::move(filter_func));
        if (!filtered_stream) {
            spdlog::critical("Failed to create filtered stream");
            return 1;
        }
        spdlog::debug("Filter applied successfully");
        
        spdlog::debug("Creating map wrapper");
        auto map_func = std::make_unique<MapFunction>("timestamp_mapper", timestampMapperFunc);
        if (!map_func) {
            spdlog::critical("Failed to create map function wrapper");
            return 1;
        }
        
        spdlog::debug("Applying map to filtered stream");
        auto mapped_stream = filtered_stream->map(std::move(map_func));
        if (!mapped_stream) {
            spdlog::critical("Failed to create mapped stream");
            return 1;
        }
        spdlog::debug("Map applied successfully");
        
        // Apply a tumbling window of 5 seconds
        spdlog::debug("Creating tumbling window");
        auto windowed_stream = mapped_stream->tumblingWindow(5000);
        if (!windowed_stream) {
            spdlog::critical("Failed to create windowed stream");
            return 1;
        }
        spdlog::debug("Tumbling window created successfully");
        
        // Apply an aggregation function (average) to the window
        spdlog::debug("Creating window aggregate function");
        auto agg_func = std::make_unique<AggregateFunction>(AggregateFunction::AggregationType::AVG, 0);
        if (!agg_func) {
            spdlog::critical("Failed to create aggregate function");
            return 1;
        }
        
        spdlog::debug("Applying window function");
        auto aggregated_stream = windowed_stream->window(std::move(agg_func));
        if (!aggregated_stream) {
            spdlog::critical("Failed to create aggregated stream");
            return 1;
        }
        spdlog::debug("Window function applied successfully");
        
        // Define allowed lateness at the stream level (applies to this stream only)
        spdlog::debug("Setting stream-specific allowed lateness");
        aggregated_stream->allowedLateness(3000); // 3 seconds
        spdlog::debug("Stream-specific allowed lateness set");
        
        // Sink the results
        spdlog::debug("Creating sink function wrapper");
        
        // Create a very simple no-op sink function to avoid any potential issues
        auto simple_sink_func = std::make_unique<SinkFunction>("simple_sink", [](std::unique_ptr<VectorRecord>&) {
            // No operation, just a simple placeholder
        });
        
        if (!simple_sink_func) {
            spdlog::critical("Failed to create sink function wrapper");
            return 1;
        }
        
        spdlog::debug("Applying sink to aggregated stream");
        
        // Use specific steps to help diagnose the segmentation fault
        spdlog::debug("Step 1: About to call writeSink");
        std::shared_ptr<Stream> sink_stream = nullptr;
        
        try {
            sink_stream = aggregated_stream->writeSink(std::move(simple_sink_func));
            spdlog::debug("Step 2: writeSink returned successfully");
        } catch (const std::exception& e) {
            spdlog::critical("Exception during writeSink: {}", e.what());
            return 1;
        } catch (...) {
            spdlog::critical("Unknown exception during writeSink");
            return 1;
        }
        
        if (!sink_stream) {
            spdlog::critical("Failed to create sink stream");
            return 1;
        }
        spdlog::debug("Sink applied successfully");
        
        // Add the source stream to the environment
        spdlog::debug("Adding file source stream to environment");
        env.addStream(file_source);
        spdlog::debug("File source stream added to environment");
        
        // Initialize the source
        spdlog::debug("Initializing file source");
        try {
            file_source->Init();
            spdlog::debug("File source initialized successfully");
        } catch (const std::exception& e) {
            spdlog::critical("Exception during file source initialization: {}", e.what());
            return 1;
        } catch (...) {
            spdlog::critical("Unknown exception during file source initialization");
            return 1;
        }
        
        // Execute the stream processing
        spdlog::debug("Starting stream execution");
        try {
            env.execute();
            spdlog::debug("Stream execution completed");
        } catch (const std::exception& e) {
            spdlog::critical("Exception during stream execution: {}", e.what());
            return 1;
        } catch (...) {
            spdlog::critical("Unknown exception during stream execution");
            return 1;
        }
        
        // Print metrics after execution
        spdlog::debug("Getting metrics");
        auto metrics = env.getMetrics();
        for (const auto& [name, value] : metrics) {
            spdlog::info("Metric {}: {}", name, value);
        }
        
        spdlog::info("Windowed streaming example finished");
        
        return 0;
    } 
    catch (const std::exception& e) {
        spdlog::critical("Global exception: {}", e.what());
        return 1;
    } 
    catch (...) {
        spdlog::critical("Unknown global exception");
        return 1;
    }
}