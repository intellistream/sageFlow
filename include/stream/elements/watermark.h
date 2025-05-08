#pragma once

#include "common/base_types.h"
#include "stream/elements/stream_element.h"

namespace candy {

/**
 * @brief Represents a watermark signal in the stream
 * 
 * Watermarks are special elements that indicate progress of event time
 * in a stream processing system. They help with triggering time-based
 * operations and handling out-of-order events.
 */
class Watermark : public StreamElement {
public:
    explicit Watermark(timestamp_t time)
        : StreamElement(StreamElementType::WATERMARK), watermark_time_(time) {
    }

    timestamp_t getTimestamp() const { return watermark_time_; }

private:
    timestamp_t watermark_time_; // The watermark's timestamp
};

}  // namespace candy