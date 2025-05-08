#ifndef SLIDING_WINDOW_H
#define SLIDING_WINDOW_H

#include "stream/time/window.h"

namespace candy {

/**
 * @brief Sliding window implementation
 * 
 * Sliding windows cover a fixed time period but slide by a smaller interval,
 * creating overlap between consecutive windows.
 */
class SlidingWindow : public Window {
 public:
  /**
   * @brief Constructor for a sliding window
   * @param start The window start timestamp
   * @param end The window end timestamp
   * @param size The size of the window
   * @param slide The slide interval of the window
   */
  SlidingWindow(timestamp_t start, timestamp_t end, timestamp_t size, timestamp_t slide)
      : Window(start, end), size_(size), slide_(slide) {}

  /**
   * @brief Get the window size
   * @return The size of the window
   */
  auto getSize() const -> timestamp_t { return size_; }

  /**
   * @brief Get the slide interval
   * @return The slide interval of the window
   */
  auto getSlide() const -> timestamp_t { return slide_; }

 private:
  timestamp_t size_;   // Size of the window
  timestamp_t slide_;  // Slide interval of the window
};

}  // namespace candy

#endif  // SLIDING_WINDOW_H