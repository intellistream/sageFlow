#include "stream/time/watermark_generator.h"
#include "stream/stream_environment.h"
#include <spdlog/spdlog.h>
#include <chrono>
#include <thread>

namespace candy {

WatermarkGenerator::WatermarkGenerator(std::string name, StreamEnvironment* stream_environment)
    : name_(std::move(name)),
      watermark_interval_ms_(1000),
      watermark_delay_ms_(500),
      max_event_time_(0),
      running_(false),
      stream_environment_(stream_environment) {
    spdlog::debug("WatermarkGenerator created for source '{}'", name_);
}

WatermarkGenerator::~WatermarkGenerator() {
    // Make sure to stop the watermark generation thread before destruction
    stopGeneration();
}

void WatermarkGenerator::startGeneration() {
    if (running_.exchange(true)) {
        spdlog::warn("Watermark generation for source '{}' already running", name_);
        return;
    }
    
    spdlog::info("Starting watermark generation for source '{}' with interval {}ms", 
               name_, watermark_interval_ms_);
    
    // Start the watermark generation thread
    watermark_thread_ = std::thread([this]() {
        while (running_) {
            // Generate and emit a watermark
            auto watermark = generateWatermark();
            
            if (stream_environment_) {
                // Notify the stream environment about the new watermark
                spdlog::debug("Source '{}' generated watermark: {}", name_, watermark.getTimestamp());
                
                // If we have a source operator defined, propagate the watermark through it
                if (source_operator_) {
                    stream_environment_->processWatermark(watermark, source_operator_);
                }
            }
            
            // Sleep for the specified interval
            std::this_thread::sleep_for(std::chrono::milliseconds(watermark_interval_ms_));
        }
    });
    
    spdlog::info("Watermark generation started for source '{}'", name_);
}

void WatermarkGenerator::stopGeneration() {
    if (!running_.exchange(false)) {
        return;
    }
    
    spdlog::info("Stopping watermark generation for source '{}'", name_);
    
    // Wait for the watermark thread to finish
    if (watermark_thread_.joinable()) {
        watermark_thread_.join();
    }
    
    spdlog::info("Watermark generation stopped for source '{}'", name_);
}

Watermark WatermarkGenerator::generateWatermark() {
    // Generate a watermark based on the maximum observed event time
    // Subtract the delay to allow for out-of-order events
    timestamp_t watermark_time = max_event_time_ - watermark_delay_ms_;
    
    // Ensure the watermark is never negative
    watermark_time = std::max(watermark_time, static_cast<timestamp_t>(0));
    
    return Watermark(watermark_time);
}

} // namespace candy