#include "stream/data_stream_source/data_stream_source.h"
#include "stream/stream_environment.h"
#include <spdlog/spdlog.h>
#include <chrono>
#include <thread>
#include <iostream>

namespace candy {

DataStreamSource::DataStreamSource(std::string name, DataStreamSourceType type)
    : Stream(std::move(name)),
      type_(type) {
    spdlog::debug("DataStreamSource {} created with type {}", name_, static_cast<int>(type_));
}

DataStreamSource::~DataStreamSource() {
    // Make sure to stop the watermark generation before destruction
    StopWatermarkGeneration();
    
    // Clear the stream_environment_ pointer for safety
    stream_environment_ = nullptr;
}

auto DataStreamSource::getType() const -> DataStreamSourceType {
    return type_;
}

void DataStreamSource::setType(DataStreamSourceType type) {
    type_ = type;
}

void DataStreamSource::setStreamEnvironment(StreamEnvironment& environment) {
    stream_environment_ = &environment;
    
    // Create the watermark generator if it doesn't exist
    if (!watermark_generator_ && stream_environment_) {
        watermark_generator_ = std::make_unique<WatermarkGenerator>(name_, stream_environment_);
    }
}

void DataStreamSource::StartWatermarkGeneration() {
    if (!watermark_generator_) {
        spdlog::warn("Cannot start watermark generation for source {} - watermark generator not initialized", name_);
        return;
    }
    
    watermark_generator_->startGeneration();
}

void DataStreamSource::StopWatermarkGeneration() {
    if (watermark_generator_) {
        watermark_generator_->stopGeneration();
    }
}

} // namespace candy
