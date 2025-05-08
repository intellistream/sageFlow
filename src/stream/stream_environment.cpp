#include "stream/stream_environment.h"

#include <fstream>
#include <spdlog/spdlog.h>
#include <thread>
#include <toml++/toml.h>
#include <algorithm>
#include <iostream>
#include <limits>

#include "operator/filter_operator.h"
#include "operator/join_operator.h"
#include "operator/map_operator.h"
#include "operator/operator.h"
#include "operator/sink_operator.h"
#include "operator/topk_operator.h"
#include "operator/window_operator.h"
#include "stream/elements/end_of_stream.h"
#include "stream/time/punctuation.h"

namespace candy {

ConfigMap StreamEnvironment::loadConfiguration(const std::string& file_path) {
    ConfigMap config_map;
    try {
        auto config = toml::parse_file(file_path);
        
        // Parse top-level configuration items
        for (const auto& [key, value] : config) {
            // Only process top-level values, skip tables which will be processed specially below
            if (value.is_string() && !value.is_table()) {
                config_map.setValue(key.str(), ConfigType::STRING, value.as_string()->get());
            } else if (value.is_integer() && !value.is_table()) {
                config_map.setValue(key.str(), ConfigType::I64, value.as_integer()->get());
            } else if (value.is_floating_point() && !value.is_table()) {
                config_map.setValue(key.str(), ConfigType::DOUBLE, value.as_floating_point()->get());
            }
        }
        
        // Parse general configuration
        if (auto general = config.get("general")) {
            if (auto lateness = general->as_table()->get("allowed_lateness")) {
                int64_t lat_val = lateness->as_integer()->get();
                ConfigValue lat_config_val(lat_val);
                config_map.setValue("allowed_lateness", ConfigType::I64, lat_config_val);
            }
        }
        
        // Parse stream-specific configuration
        if (auto streams = config.get("streams")) {
            // Use foreach since some TOML++ versions don't have items()
            for (auto&& [stream_name, stream_config] : *streams->as_table()) {
                // Add stream-specific configuration to the map
                std::string prefix = "streams." + std::string(stream_name.str()) + ".";
                
                if (auto watermark_interval = stream_config.as_table()->get("watermark_interval")) {
                    int64_t wm_val = watermark_interval->as_integer()->get();
                    ConfigValue wm_config_val(wm_val);
                    config_map.setValue(prefix + "watermark_interval", ConfigType::I64, wm_config_val);
                }
                
                if (auto buffer_size = stream_config.as_table()->get("buffer_size")) {
                    int64_t buf_val = buffer_size->as_integer()->get();
                    ConfigValue buf_config_val(buf_val);
                    config_map.setValue(prefix + "buffer_size", ConfigType::I64, buf_config_val);
                }
            }
        }
        
        // Parse monitoring configuration
        if (auto monitoring = config.get("monitoring")) {
            if (auto enabled = monitoring->as_table()->get("enabled")) {
                int64_t value = enabled->as_boolean()->get() ? 1 : 0;
                ConfigValue enabled_config_val(value);
                config_map.setValue("monitoring.enabled", ConfigType::I64, enabled_config_val);
            }
            
            if (auto interval = monitoring->as_table()->get("metrics_interval")) {
                int64_t int_val = interval->as_integer()->get();
                ConfigValue int_config_val(int_val);
                config_map.setValue("monitoring.metrics_interval", ConfigType::I64, int_config_val);
            }
        }
        
        spdlog::info("Configuration loaded from {}", file_path);
    } catch (const std::exception& e) {
        spdlog::error("Error loading configuration from {}: {}", file_path, e.what());
    }
    
    return config_map;
}

void StreamEnvironment::addStream(std::shared_ptr<Stream> stream) {
    streams_.push_back(stream);
    spdlog::debug("Stream added to environment: {}", stream->name_);
}

void StreamEnvironment::registerOperator(std::shared_ptr<Operator> op) {
    operators_.push_back(op);
    
    // Initialize the operator's watermark to 0
    operator_watermarks_[op] = 0;
    
    spdlog::debug("Operator registered with StreamEnvironment: {}", 
                 static_cast<int>(op->getType()));
}

void StreamEnvironment::start() {
    if (running_.exchange(true)) {
        spdlog::warn("StreamEnvironment already running");
        return;
    }
    
    // Build operator graph if needed
    buildOperatorGraph();
    
    // Open all operators
    for (auto& op : operators_) {
        op->open();
    }
    
    spdlog::info("StreamEnvironment started");
}

void StreamEnvironment::stop() {
    if (!running_.exchange(false)) {
        spdlog::warn("StreamEnvironment already stopped");
        return;
    }
    
    // Close all operators
    for (auto& op : operators_) {
        op->close();
    }
    
    spdlog::info("StreamEnvironment stopped");
}

void StreamEnvironment::processWatermark(const Watermark& watermark, std::shared_ptr<Operator> source_op) {
    if (!running_) {
        spdlog::warn("Cannot process watermark: StreamEnvironment not running");
        return;
    }
    
    // Update the watermark for the specified operator
    operator_watermarks_[source_op] = watermark.getTimestamp();
    
    // Create a Punctuation element for the watermark
    auto punctuation = std::make_shared<Punctuation>(PunctuationType::WATERMARK, watermark.getTimestamp());
    
    // Propagate the watermark to all child operators
    for (const auto& child : source_op->getChildren()) {
        if (child) {
            // Use the punctuation as a StreamElement
            StreamElement& element = *punctuation;
            // Emit it to the child operator
            source_op->emit(0, element);
        }
    }
    
    // Update global watermark
    updateGlobalWatermark();
    
    // Record watermark progress
    spdlog::debug("Watermark processed: {} for operator type {}", 
                 watermark.getTimestamp(), 
                 static_cast<int>(source_op->getType()));
}

timestamp_t StreamEnvironment::getCurrentWatermark() const {
    return global_watermark_.load();
}

void StreamEnvironment::processEndOfStream(std::shared_ptr<Operator> source_op) {
    if (!running_) {
        spdlog::warn("Cannot process EndOfStream: StreamEnvironment not running");
        return;
    }

    spdlog::info("Processing EndOfStream for operator type {}", static_cast<int>(source_op->getType()));

    // Create an EndOfStream element
    auto end_of_stream = std::make_shared<EndOfStream>();

    // Propagate the EndOfStream to all child operators
    for (const auto& child : source_op->getChildren()) {
        if (child) {
            StreamElement& element = *end_of_stream;
            source_op->emit(0, element);
        }
    }

    // Check if all operators have received EndOfStream
    bool all_ended = true;
    for (const auto& op : operators_) {
        if (!op->hasReceivedEndOfStream()) {
            all_ended = false;
            break;
        }
    }

    if (all_ended) {
        spdlog::info("All operators have received EndOfStream. Stopping StreamEnvironment.");
        stop();
    }
}

void StreamEnvironment::updateMetric(const std::string& name, double value) {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    metrics_[name] = value;
}

void StreamEnvironment::execute() {
    if (streams_.empty()) {
        spdlog::warn("No streams to execute");
        return;
    }
    
    spdlog::info("Executing stream processing");
    
    // Start processing - this begins processing
    start();
    
    // In a real application, we would have a control loop here
    // For now, we'll just simulate a processing duration
    spdlog::info("Running stream operators...");
    
    // For demo purposes, sleep for a while to allow processing
    // In a real implementation, we would use proper termination conditions
    std::this_thread::sleep_for(std::chrono::seconds(5));
    
    // Stop processing when done
    stop();
    
    // Show some metrics
    auto metrics = getMetrics();
    for (const auto& [key, value] : metrics) {
        spdlog::info("Metric {}: {}", key, value);
    }
    
    spdlog::info("Stream processing finished");
}

void StreamEnvironment::updateGlobalWatermark() {
    // The global watermark is the minimum across all operator watermarks
    timestamp_t min_watermark = std::numeric_limits<timestamp_t>::max();
    
    for (const auto& [op, watermark] : operator_watermarks_) {
        min_watermark = std::min(min_watermark, watermark);
    }
    
    // Update the global watermark
    global_watermark_.store(min_watermark);
    
    // Log the global watermark update
    spdlog::debug("Global watermark updated: {}", min_watermark);
    
    // Update the watermark lag metric
    // Assuming the current time represents processing time
    auto now = std::chrono::system_clock::now().time_since_epoch().count();
    updateMetric("watermark_lag", static_cast<double>(now - min_watermark));
}

// Private method to build the operator graph from streams
void StreamEnvironment::buildOperatorGraph() {
    spdlog::info("Building operator graph from {} registered streams", streams_.size());
    
    if (streams_.empty()) {
        spdlog::warn("No streams registered, operator graph will be empty");
        return;
    }
    
    // Clear existing operators to rebuild the graph
    operators_.clear();
    operator_watermarks_.clear();
    
    // Map to track which streams already have operators
    std::unordered_map<std::shared_ptr<Stream>, std::shared_ptr<Operator>> stream_operators;
    
    // Process each stream and create corresponding operators
    for (const auto& stream : streams_) {
        // Skip if we've already created an operator for this stream
        if (stream_operators.find(stream) != stream_operators.end()) {
            continue;
        }
        
        // Skip streams without functions
        if (!stream || !stream->function_) {
            spdlog::warn("Stream '{}' is null or has no function, skipping", stream ? stream->name_ : "unknown");
            continue;
        }
        
        spdlog::info("Stream '{}' has function of type {} during operator graph construction", stream->name_, static_cast<int>(stream->function_->getType()));
        
        // Create the appropriate operator type based on the function type
        std::shared_ptr<Operator> op;
        FunctionType func_type = stream->function_->getType();
        OperatorType op_type;
        
        // Map function types to operator types with precise case matching
        if (func_type == FunctionType::Map) {
            op_type = OperatorType::MAP;
        } else if (func_type == FunctionType::Filter) {
            op_type = OperatorType::FILTER;
        } else if (func_type == FunctionType::Window) {
            op_type = OperatorType::WINDOW;
        } else if (func_type == FunctionType::Join) {
            op_type = OperatorType::JOIN;
        } else if (func_type == FunctionType::Sink) {
            op_type = OperatorType::SINK;
        } else if (func_type == FunctionType::Topk) {
            op_type = OperatorType::TOPK;
        } else {
            spdlog::warn("Unsupported function type for stream '{}'", stream->name_);
            continue;
        }
        
        // Create the base operator with the right type
        op = std::make_shared<Operator>(op_type);
        if (!op) {
            spdlog::error("Failed to create operator for stream '{}'", stream->name_);
            continue;
        }
        
        // Set the function in the operator
        // Note: This might lead to stream's function being null afterward
        op->setFunction(std::move(stream->function_));
        
        // Register the operator with the environment
        operators_.push_back(op);
        operator_watermarks_[op] = 0;
        
        // Associate the stream with its operator
        stream_operators[stream] = op;
        
        spdlog::debug("Created operator type {} for stream '{}'", 
                     static_cast<int>(op->getType()), stream->name_);
    }
    
    // Connect the operators to build the graph
    for (const auto& stream : streams_) {
        auto source_op_iter = stream_operators.find(stream);
        if (source_op_iter == stream_operators.end()) {
            continue;  // Skip streams without operators
        }
        
        auto source_op = source_op_iter->second;
        
        // Connect to downstream streams
        for (const auto& downstream_stream : stream->streams_) {
            auto target_op_iter = stream_operators.find(downstream_stream);
            if (target_op_iter == stream_operators.end()) {
                continue;  // Skip if downstream stream has no operator
            }
            
            auto target_op = target_op_iter->second;
            
            // Connect source to target
            source_op->addChild(target_op);
            
            spdlog::debug("Connected stream '{}' to '{}'", 
                         stream->name_, downstream_stream->name_);
        }
    }
    
    // Handle special case for join operators
    for (auto& op : operators_) {
        if (op->getType() == OperatorType::JOIN && op->getChildren().size() < 2) {
            spdlog::warn("Join operator has fewer than 2 inputs, join may not function correctly");
        }
    }
    
    // Log operator graph structure
    for (const auto& op : operators_) {
        std::string children_str = "";
        for (const auto& child : op->getChildren()) {
            children_str += std::to_string(static_cast<int>(child->getType())) + ", ";
        }
        
        spdlog::debug("Operator {} - Children: [{}]", 
                     static_cast<int>(op->getType()),
                     children_str.empty() ? "none" : children_str);
    }
    
    spdlog::info("Built operator graph with {} operators", operators_.size());
}

// Helper method to create operators from streams
void StreamEnvironment::createOperators(std::shared_ptr<Stream> stream) {
    // Create an operator for this stream based on its function type
    std::shared_ptr<Operator> op = nullptr;
    
    if (!stream->function_) {
        spdlog::warn("Stream {} has no function", stream->name_);
        return;
    }
    
    // Create the appropriate operator based on the function type
    switch (stream->function_->getType()) {
        case FunctionType::Filter: {
            op = std::make_shared<FilterOperator>(std::move(stream->function_));
            break;
        }
        case FunctionType::Map: {
            op = std::make_shared<MapOperator>(std::move(stream->function_));
            break;
        }
        case FunctionType::Join: {
            op = std::make_shared<JoinOperator>(std::move(stream->function_));
            break;
        }
        case FunctionType::Sink: {
            op = std::make_shared<SinkOperator>(std::move(stream->function_));
            break;
        }
        case FunctionType::Topk: {
            // Use reference to concurrency_manager instead of shared_ptr
            op = std::make_shared<TopkOperator>(std::move(stream->function_), *concurrency_manager_);
            break;
        }
        case FunctionType::Window: {
            // Determine if this is a tumbling or sliding window based on the stream name
            // This is a simplification - real implementation would need more context
            if (stream->name_.find("tumbling") != std::string::npos) {
                // Extract window size from stream (simplified)
                timestamp_t window_size = 1000; // Default 1 second in milliseconds
                auto window_func = std::move(stream->function_);
                op = std::make_shared<TumblingWindowOperator>(std::move(window_func), window_size);
            } else if (stream->name_.find("sliding") != std::string::npos) {
                // Extract window size and slide from stream (simplified)
                timestamp_t window_size = 1000; // Default 1 second
                timestamp_t window_slide = 500; // Default 0.5 second
                auto window_func = std::move(stream->function_);
                op = std::make_shared<SlidingWindowOperator>(std::move(window_func), window_size, window_slide);
            } else {
                // Generic window
                auto window_func = std::move(stream->function_);
                // For now, default to tumbling window
                op = std::make_shared<TumblingWindowOperator>(std::move(window_func), 1000);
            }
            
            // Set allowed lateness for the window operator
            if (auto window_op = std::dynamic_pointer_cast<WindowOperator>(op)) {
                window_op->setAllowedLateness(stream->allowed_lateness_);
            }
            break;
        }
        default: {
            spdlog::error("Unknown function type for stream {}", stream->name_);
            return;
        }
    }
    
    // Add the operator to our list
    if (op) {
        operators_.push_back(op);
        
        // Connect this operator to its children
        for (const auto& child_stream : stream->streams_) {
            // Recursively create operators for child streams if needed
            createOperators(child_stream);
            
            // Find the operator for the child stream
            for (const auto& child_op : operators_) {
                // This is a simplified approach to connect operators
                // In a real implementation, you'd need more context to know which operators
                // correspond to which streams
                op->addChild(child_op);
            }
        }
    }
}

}  // namespace candy
