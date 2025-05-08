#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "common/data_types.h"
#include "stream/elements/watermark.h"

namespace candy {

// Forward declarations
class StreamEnvironment;
class Operator;

/**
 * @brief A class that generates watermarks for data streams
 * 
 * The WatermarkGenerator periodically emits watermarks that indicate
 * the progress of event time in a stream processing system.
 * It helps with triggering time-based operations like windows.
 */
class WatermarkGenerator {
 public:
  /**
   * @brief Constructor for WatermarkGenerator
   * @param name The name of the source the generator is associated with
   * @param stream_environment Pointer to the StreamEnvironment to notify about watermarks
   */
  WatermarkGenerator(std::string name, StreamEnvironment* stream_environment);
  
  /**
   * @brief Destructor ensures watermark generation is stopped
   */
  ~WatermarkGenerator();

  /**
   * @brief Set the watermark interval
   * @param interval_ms Interval in milliseconds between watermark emissions
   */
  void setWatermarkInterval(timestamp_t interval_ms) { watermark_interval_ms_ = interval_ms; }

  /**
   * @brief Get the watermark interval
   * @return Interval in milliseconds between watermark emissions
   */
  auto getWatermarkInterval() const -> timestamp_t { return watermark_interval_ms_; }

  /**
   * @brief Set the delay for the watermark compared to the maximum event time
   * @param delay_ms Delay in milliseconds
   */
  void setWatermarkDelay(timestamp_t delay_ms) { watermark_delay_ms_ = delay_ms; }

  /**
   * @brief Get the watermark delay 
   * @return Delay in milliseconds
   */
  auto getWatermarkDelay() const -> timestamp_t { return watermark_delay_ms_; }

  /**
   * @brief Start the periodic watermark generation
   */
  void startGeneration();

  /**
   * @brief Stop the periodic watermark generation
   */
  void stopGeneration();

  /**
   * @brief Generate a watermark based on the current event time
   * @return A watermark representing the current progress of the stream
   */
  auto generateWatermark() -> Watermark;

  /**
   * @brief Update the maximum observed event time
   * @param event_time Timestamp of the event
   */
  void updateMaxEventTime(timestamp_t event_time) { 
    max_event_time_ = std::max(max_event_time_, event_time); 
  }

  /**
   * @brief Set the source operator for watermark propagation
   * @param source_op The operator representing the source
   */
  void setSourceOperator(std::shared_ptr<Operator> source_op) {
    source_operator_ = source_op;
  }

 private:
  // Identification
  std::string name_;
  
  // Watermark generation settings
  timestamp_t watermark_interval_ms_ = 1000;  // Generate watermark every 1 second by default
  timestamp_t watermark_delay_ms_ = 500;      // Delay watermark by 500ms behind max event time by default
  timestamp_t max_event_time_ = 0;            // Track the maximum observed event time

  // Watermark generation thread
  std::thread watermark_thread_;
  std::atomic<bool> running_{false};

  // Stream environment for watermark notifications
  StreamEnvironment* stream_environment_ = nullptr;
  
  // Source operator for propagating watermarks through the graph
  std::shared_ptr<Operator> source_operator_ = nullptr;
};

}  // namespace candy