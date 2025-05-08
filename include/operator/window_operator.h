#ifndef WINDOW_OPERATOR_H
#define WINDOW_OPERATOR_H

#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

#include "operator/operator.h"
#include "stream/time/window.h"

namespace candy {

/**
 * @brief Base class for window operators
 */
class WindowOperator : public Operator {
 public:
  /**
   * @brief Constructor for WindowOperator
   * @param function Function to apply to each window
   */
  explicit WindowOperator(std::unique_ptr<Function> function);

  ~WindowOperator() override = default;

  /**
   * @brief Process a data element, assign to windows, and potentially trigger window evaluation
   * @param element The data element to process
   * @param slot Input slot (for multi-input operators)
   * @return True if processing is successful
   */
  auto processDataElement(DataElement& element, int slot = 0) -> bool override;

  /**
   * @brief Process a watermark, trigger window evaluation, and clean up expired windows
   * @param watermark The watermark to process
   * @return True if watermark processing is successful
   */
  virtual auto processWatermark(timestamp_t watermark_time) -> bool;

  /**
   * @brief Set the allowed lateness for records
   * @param allowed_lateness Maximum allowed lateness in time units
   */
  void setAllowedLateness(timestamp_t allowed_lateness) { allowed_lateness_ = allowed_lateness; }

 protected:
  /**
   * @brief Assign a data element to its respective window(s)
   * @param element The data element to assign
   * @return List of windows the element belongs to
   */
  virtual auto assignWindows(const DataElement& element) const -> std::vector<std::shared_ptr<Window>> = 0;

  /**
   * @brief Trigger evaluation of a window
   * @param window The window to evaluate
   */
  virtual void triggerWindow(const std::shared_ptr<Window>& window) = 0;

  /**
   * @brief Check if a window should be triggered by the watermark
   * @param window The window to check
   * @param watermark Current watermark
   * @return True if the window should be triggered
   */
  auto shouldTrigger(const std::shared_ptr<Window>& window, timestamp_t watermark) const -> bool {
    return watermark >= window->getEnd();
  }

  /**
   * @brief Check if a window state should be cleared due to watermark advancement
   * @param window The window to check
   * @param watermark Current watermark
   * @return True if the window state should be cleared
   */
  bool shouldClear(const std::shared_ptr<Window>& window, timestamp_t watermark) const {
    return watermark > window->getEnd() + allowed_lateness_;
  }

  // Map of windows to their buffered elements
  std::unordered_map<std::shared_ptr<Window>, std::vector<DataElement>,
                     std::function<size_t(const std::shared_ptr<Window>&)>>
      window_buffers_;

  // Current watermark
  timestamp_t current_watermark_ = 0;

  // Maximum allowed lateness
  timestamp_t allowed_lateness_ = 0;
};

/**
 * @brief Tumbling window operator
 */
class TumblingWindowOperator : public WindowOperator {
 public:
  /**
   * @brief Constructor for TumblingWindowOperator
   * @param function Function to apply to each window
   * @param size Window size in time units
   */
  TumblingWindowOperator(std::unique_ptr<Function> function, timestamp_t size);

 protected:
  /**
   * @brief Assign a data element to its tumbling window
   * @param element The data element to assign
   * @return The tumbling window for the element
   */
  auto assignWindows(const DataElement& element) const -> std::vector<std::shared_ptr<Window>> override;

  /**
   * @brief Trigger evaluation of a tumbling window
   * @param window The window to evaluate
   */
  void triggerWindow(const std::shared_ptr<Window>& window) override;

 private:
  timestamp_t window_size_;
};

/**
 * @brief Sliding window operator
 */
class SlidingWindowOperator : public WindowOperator {
 public:
  /**
   * @brief Constructor for SlidingWindowOperator
   * @param function Function to apply to each window
   * @param size Window size in time units
   * @param slide Window slide interval in time units
   */
  SlidingWindowOperator(std::unique_ptr<Function> function, timestamp_t size, timestamp_t slide);

 protected:
  /**
   * @brief Assign a data element to its sliding windows
   * @param element The data element to assign
   * @return List of sliding windows for the element
   */
  std::vector<std::shared_ptr<Window>> assignWindows(const DataElement& element) const override;

  /**
   * @brief Trigger evaluation of a sliding window
   * @param window The window to evaluate
   */
  void triggerWindow(const std::shared_ptr<Window>& window) override;

 private:
  timestamp_t window_size_;
  timestamp_t window_slide_;
};

}  // namespace candy

#endif  // WINDOW_OPERATOR_H