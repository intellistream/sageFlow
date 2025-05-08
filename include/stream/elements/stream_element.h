#pragma once

#include "common/base_types.h"

namespace candy {

// Stream element type enum
enum class StreamElementType { 
  DATA, 
  WATERMARK, 
  END_OF_STREAM, 
  CHECKPOINT_BARRIER
};

/**
 * @brief Base class for all elements flowing through the stream
 * 
 * StreamElement is the abstract base class for different types of elements
 * that can flow through the stream processing pipeline, including data
 * records, watermarks, and control signals.
 */
class StreamElement {
 public:
  explicit StreamElement(StreamElementType type) : type_(type) {}

  virtual ~StreamElement() = default;

  virtual auto getType() const -> StreamElementType { return type_; }

 private:
  StreamElementType type_; // Type identifier for the element
};

}  // namespace candy