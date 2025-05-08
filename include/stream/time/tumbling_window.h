#ifndef TUMBLING_WINDOW_H
#define TUMBLING_WINDOW_H

#include "stream/time/window.h"

namespace candy {

/**
 * @brief Tumbling window implementation
 * 
 * Tumbling windows cover a fixed time period with no overlap between
 * consecutive windows. They are defined by their size.
 */
class TumblingWindow : public Window {
 public:
  /**
   * @brief Constructor for a tumbling window
   * @param start The window start timestamp
   * @param end The window end timestamp
   * @param size The size of the window
   */
  TumblingWindow(timestamp_t start, timestamp_t end, timestamp_t size)
      : Window(start, end), size_(size) {}

  /**
   * @brief Get the window size
   * @return The size of the window
   */
  auto getSize() const -> timestamp_t { return size_; }

 private:
  timestamp_t size_;  // Size of the window
};

}  // namespace candy

#endif  // TUMBLING_WINDOW_H