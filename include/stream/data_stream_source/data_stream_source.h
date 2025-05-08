#pragma once
#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "common/data_types.h"
#include "stream/stream.h"
#include "stream/time/watermark_generator.h"

namespace candy {
// Forward declaration
class StreamEnvironment;

enum class DataStreamSourceType {  // NOLINT
  None,
  File,
};

/**
 * @brief Base class for data stream sources
 *
 * Data stream sources generate records and watermarks for stream processing.
 */
class DataStreamSource : public Stream {
 public:
  DataStreamSource(std::string name, DataStreamSourceType type);
  ~DataStreamSource() override;

  auto getType() const -> DataStreamSourceType;

  void setType(DataStreamSourceType type);

  /**
   * @brief Set the maximum buffer size for the stream
   * @param limit Maximum buffer size in bytes
   */
  void setBufferSizeLimit(size_t limit) { buffer_size_limit_ = limit; }

  /**
   * @brief Get the maximum buffer size for the stream
   * @return Maximum buffer size in bytes
   */
  auto getBufferSizeLimit() const -> size_t { return buffer_size_limit_; }

  /**
   * @brief Set the watermark interval for the source
   * @param interval_ms Interval in milliseconds between watermark emissions
   */
  void setWatermarkInterval(timestamp_t interval_ms) {
    if (watermark_generator_) {
      watermark_generator_->setWatermarkInterval(interval_ms);
    }
  }

  /**
   * @brief Get the watermark interval for the source
   * @return Interval in milliseconds between watermark emissions
   */
  auto getWatermarkInterval() const -> timestamp_t {
    return watermark_generator_ ? watermark_generator_->getWatermarkInterval() : 0;
  }

  /**
   * @brief Set the delay for the watermark compared to the maximum event time
   * @param delay_ms Delay in milliseconds
   */
  void setWatermarkDelay(timestamp_t delay_ms) {
    if (watermark_generator_) {
      watermark_generator_->setWatermarkDelay(delay_ms);
    }
  }

  /**
   * @brief Get the next record from the stream
   * @return Pointer to the next VectorRecord, or nullptr if end of stream
   */
  virtual auto Next() -> std::unique_ptr<VectorRecord> = 0;

  /**
   * @brief Initialize the data stream source
   */
  virtual void Init() {}

  /**
   * @brief Start the periodic watermark generation
   */
  virtual void StartWatermarkGeneration();

  /**
   * @brief Stop the periodic watermark generation
   */
  virtual void StopWatermarkGeneration();

  /**
   * @brief Set the stream environment to be notified of watermarks
   * @param environment Reference to the StreamEnvironment instance
   */
  void setStreamEnvironment(StreamEnvironment& environment);

  /**
   * @brief Update the maximum observed event time
   * @param event_time Timestamp of the event
   */
  void updateMaxEventTime(timestamp_t event_time) {
    if (watermark_generator_) {
      watermark_generator_->updateMaxEventTime(event_time);
    }
  }

 protected:
  DataStreamSourceType type_ = DataStreamSourceType::None;
  size_t buffer_size_limit_ = (1 << 20);  // 1MB

  // Watermark generator
  std::unique_ptr<WatermarkGenerator> watermark_generator_;

  // Stream environment for watermark notifications - raw pointer
  StreamEnvironment* stream_environment_ = nullptr;
};
}  // namespace candy