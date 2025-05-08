#ifndef WINDOW_H
#define WINDOW_H

#include "common/base_types.h"

namespace candy {

/**
 * @brief Base class for time window definitions
 * 
 * Defines the common interface and behavior for all window types in
 * the stream processing system, establishing start and end times.
 */
class Window {
 public:
  Window(timestamp_t start, timestamp_t end) : start_(start), end_(end) {}

  virtual ~Window() = default;

  auto getStart() const -> timestamp_t { return start_; }

  auto getEnd() const -> timestamp_t { return end_; }

  // Comparator for windows
  auto operator==(const Window& other) const -> bool { return start_ == other.start_ && end_ == other.end_; }

 protected:
  timestamp_t start_;  // Start time of the window (inclusive)
  timestamp_t end_;    // End time of the window (exclusive)
};

}  // namespace candy

#endif  // WINDOW_H