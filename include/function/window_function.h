#ifndef WINDOW_FUNCTION_H
#define WINDOW_FUNCTION_H

#include <vector>

#include "common/data_types.h"
#include "function/function.h"

namespace candy {

/**
 * @brief Base class for window functions
 *
 * Window functions process groups of records within a time window.
 */
class WindowFunction : public Function {
 public:
  WindowFunction() : Function(FunctionType::Window) {}

  ~WindowFunction() override = default;

  /**
   * @brief Process a window of records
   * @param elements Vector of data elements in the window
   * @param window_start Start timestamp of the window
   * @param window_end End timestamp of the window
   * @return Processed result
   */
  virtual auto process(const std::vector<DataElement>& elements, timestamp_t window_start, timestamp_t window_end) -> DataElement = 0;
};

/**
 * @brief Function for aggregating values in a window
 */
class AggregateFunction : public WindowFunction {
 public:
  // Different types of aggregations
  enum class AggregationType { SUM, COUNT, MIN, MAX, AVG };

  /**
   * @brief Constructor for AggregateFunction
   * @param type Type of aggregation to perform
   * @param field_index Index of the field to aggregate (if applicable)
   */
  explicit AggregateFunction(AggregationType type, int field_index = 0);

  /**
   * @brief Process aggregation on a window of records
   * @param elements Vector of data elements in the window
   * @param window_start Start timestamp of the window
   * @param window_end End timestamp of the window
   * @return Aggregated result
   */
  auto process(const std::vector<DataElement>& elements, timestamp_t window_start, timestamp_t window_end) -> DataElement override;

 private:
  AggregationType type_;
  int field_index_;
};

}  // namespace candy

#endif  // WINDOW_FUNCTION_H