#ifndef PUNCTUATION_H
#define PUNCTUATION_H

#include "common/base_types.h"
#include "stream/elements/stream_element.h"

namespace candy {

/**
 * @brief Punctuation type enum for different punctuation signals
 */
enum class PunctuationType {
  WATERMARK,  // Indicates a watermark in the stream
  END         // Indicates end of stream
};

/**
 * @brief Represents a punctuation in the stream
 * 
 * Punctuation elements are special control signals that provide 
 * meta-information about the stream, such as watermarks.
 */
class Punctuation : public StreamElement {
 public:
  Punctuation(PunctuationType type, timestamp_t time)
      : StreamElement(StreamElementType::WATERMARK), punctuation_type_(type), punctuation_time_(time) {}

  // Correctly override the base class getType() method
  auto getType() const -> StreamElementType override { return StreamElementType::WATERMARK; }
  
  // Get the specific punctuation type (renamed to avoid conflict)
  auto getPunctuationType() const -> PunctuationType { return punctuation_type_; }
  
  auto getTimestamp() const -> timestamp_t { return punctuation_time_; }

 private:
  PunctuationType punctuation_type_;
  timestamp_t punctuation_time_;
};

}  // namespace candy

#endif  // PUNCTUATION_H