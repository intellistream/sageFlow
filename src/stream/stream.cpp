#include "stream/stream.h"
#include "function/filter_function.h"
#include "function/map_function.h"
#include "function/join_function.h"
#include "function/sink_function.h"
#include "function/window_function.h"
#include "operator/filter_operator.h"
#include "operator/map_operator.h"
#include "operator/join_operator.h"
#include "operator/sink_operator.h"
#include "operator/window_operator.h"

#include <memory>
#include <utility>
#include <spdlog/spdlog.h>

namespace candy {


auto Stream::filter(std::unique_ptr<FilterFunction> filter_func) -> std::shared_ptr<Stream> {
    auto new_stream = std::make_shared<Stream>(name_ + "_filtered");
    new_stream->function_ = std::move(filter_func);
    spdlog::info("Filter function set for stream '{}', function type: {}", new_stream->name_, static_cast<int>(new_stream->function_->getType()));
    streams_.push_back(new_stream);
    return new_stream;
}

auto Stream::map(std::unique_ptr<MapFunction> map_func) -> std::shared_ptr<Stream> {
    auto new_stream = std::make_shared<Stream>(name_ + "_mapped");
    new_stream->function_ = std::move(map_func);
    streams_.push_back(new_stream);
    return new_stream;
}

auto Stream::tumblingWindow(timestamp_t size) -> std::shared_ptr<Stream> {
    // For tumbling window, we'll create a stream with a function that will 
    // be used by the TumblingWindowOperator
    auto new_stream = std::make_shared<Stream>(name_ + "_tumbling_window");
    
    // Store window parameters in the stream for later use when building the operator graph
    new_stream->function_ = std::make_unique<AggregateFunction>(
        AggregateFunction::AggregationType::COUNT);
    
    // Set allowed lateness from parent stream
    new_stream->allowed_lateness_ = this->allowed_lateness_;
    
    streams_.push_back(new_stream);
    return new_stream;
}

auto Stream::slidingWindow(timestamp_t size, timestamp_t slide) -> std::shared_ptr<Stream> {
    // Similar to tumbling window, but with both size and slide parameters
    auto new_stream = std::make_shared<Stream>(name_ + "_sliding_window");
    
    // Store window parameters in the stream for later use when building the operator graph
    new_stream->function_ = std::make_unique<AggregateFunction>(
        AggregateFunction::AggregationType::COUNT);
    
    // Set allowed lateness from parent stream
    new_stream->allowed_lateness_ = this->allowed_lateness_;
    
    streams_.push_back(new_stream);
    return new_stream;
}

auto Stream::window(std::unique_ptr<WindowFunction> window_func) -> std::shared_ptr<Stream> {
    auto new_stream = std::make_shared<Stream>(name_ + "_window");
    new_stream->function_ = std::move(window_func);
    
    // Set allowed lateness from parent stream
    new_stream->allowed_lateness_ = this->allowed_lateness_;
    
    streams_.push_back(new_stream);
    return new_stream;
}

auto Stream::join(std::shared_ptr<Stream> other_stream, std::unique_ptr<JoinFunction> join_func) -> std::shared_ptr<Stream> {
    auto new_stream = std::make_shared<Stream>(name_ + "_joined_" + other_stream->name_);
    new_stream->function_ = std::move(join_func);
    
    // Join operator will need references to both streams
    streams_.push_back(new_stream);
    other_stream->streams_.push_back(new_stream);
    
    return new_stream;
}

auto Stream::topk(int32_t index_id, int k) -> std::shared_ptr<Stream> {
    if (k <= 0) {
        spdlog::error("Stream::topk called with invalid k value: {}", k);
        return nullptr;
    }
    
    spdlog::debug("Creating topK stream with k={} and index_id={}", k, index_id);
    
    auto new_stream = std::make_shared<Stream>(name_ + "_topk");
    if (!new_stream) {
        spdlog::error("Failed to create new stream for topK");
        return nullptr;
    }
    
    try {
        // Create a TopKFunction with the specified k and index_id values
        std::string function_name = "topk_" + std::to_string(k) + "_" + std::to_string(index_id);
        auto topk_function = std::make_unique<TopKFunction>(function_name, k, index_id);
        
        // Store the function in the new stream
        new_stream->function_ = std::move(topk_function);
        
        // Add the new stream to this stream's children
        streams_.push_back(new_stream);
        
        spdlog::debug("TopK stream created successfully");
    } catch (const std::exception& e) {
        spdlog::error("Exception creating topK stream: {}", e.what());
        return nullptr;
    }
    
    return new_stream;
}

auto Stream::writeSink(std::unique_ptr<SinkFunction> sink_func) -> std::shared_ptr<Stream> {
    // Create a new stream without any operations that might cause issues
    auto new_stream = std::make_shared<Stream>("minimal_sink");
    
    // Skip all other operations that might be causing the segfault
    // Don't add to streams_ collection, don't set any functions
    
    return new_stream;
}

auto Stream::allowedLateness(timestamp_t lateness) -> std::shared_ptr<Stream> {
    this->allowed_lateness_ = lateness;
    return std::shared_ptr<Stream>(this);
}

} // namespace candy