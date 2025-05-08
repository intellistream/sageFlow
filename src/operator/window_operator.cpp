#include "operator/window_operator.h"
#include "stream/elements/watermark.h"
#include "stream/time/tumbling_window.h"
#include "stream/time/sliding_window.h"
#include <spdlog/spdlog.h>
#include <algorithm>
#include <cmath>
#include <iostream>

namespace candy {

WindowOperator::WindowOperator(std::unique_ptr<Function> function)
    : Operator(OperatorType::WINDOW),  // Now using the correct WINDOW type
      current_watermark_(0),
      allowed_lateness_(0) {
    setFunction(std::move(function));
    
    // Initialize window buffer with custom hash function for shared_ptr<Window>
    window_buffers_ = std::unordered_map<std::shared_ptr<Window>, std::vector<DataElement>,
        std::function<size_t(const std::shared_ptr<Window>&)>>(
        10, // Initial bucket count
        [](const std::shared_ptr<Window>& window) {
            // Hash based on window start and end time
            return std::hash<timestamp_t>{}(window->getStart()) ^ 
                   std::hash<timestamp_t>{}(window->getEnd());
        }
    );
}

auto WindowOperator::processDataElement(DataElement& element, int slot) -> bool {
    if (!isOpen()) {
        spdlog::error("WindowOperator not open");
        return false;
    }
    
    // Check if this is a non-data element (e.g. a control signal)
    if (element.isEmpty()) {
        // We handle only data elements here
        // Control elements like watermarks are handled in the main process method
        spdlog::debug("WindowOperator received empty data element, ignoring");
        return false;
    }
    
    // Extract timestamp from element
    timestamp_t timestamp = 0;
    
    // If this is a record, extract timestamp
    if (element.isRecord() && element.getRecord()) {
        timestamp = element.getEventTime();
    } else {
        // Handle other element types or lists of records
        spdlog::warn("WindowOperator received unsupported element type");
        return false;
    }
    
    // Check if the element is too late (beyond allowed lateness)
    if (timestamp <= current_watermark_ - allowed_lateness_) {
        spdlog::info("Late element dropped: timestamp {} is before watermark {} - lateness {}",
                    timestamp, current_watermark_, allowed_lateness_);
        return true;  // Still return true to continue processing
    }
    
    // Assign the element to its window(s)
    auto windows = assignWindows(element);
    
    // Buffer the element in each of its windows
    for (const auto& window : windows) {
        window_buffers_[window].push_back(std::move(element));
    }
    
    return true;
}

auto WindowOperator::processWatermark(timestamp_t watermark_time) -> bool {
    // Update the current watermark
    current_watermark_ = std::max(current_watermark_, watermark_time);

    spdlog::debug("WindowOperator processing watermark: {}", watermark_time);

    // Find windows that should be triggered by this watermark
    std::vector<std::shared_ptr<Window>> windows_to_trigger;
    std::vector<std::shared_ptr<Window>> windows_to_clear;

    for (const auto& [window, buffer] : window_buffers_) {
        if (shouldTrigger(window, watermark_time)) {
            windows_to_trigger.push_back(window);
        }

        if (shouldClear(window, watermark_time)) {
            windows_to_clear.push_back(window);
        }
    }

    // Trigger windows
    for (const auto& window : windows_to_trigger) {
        triggerWindow(window);
    }

    // Clear expired windows
    for (const auto& window : windows_to_clear) {
        window_buffers_.erase(window);
        spdlog::debug("Window cleared: [{}, {})", window->getStart(), window->getEnd());
    }

    // Forward the watermark to downstream operators
    auto watermark_element = std::make_shared<Watermark>(watermark_time);
    for (size_t i = 0; i < getChildren().size(); ++i) {
        emit(i, *watermark_element);
    }

    return true;
}

// TumblingWindowOperator implementation

TumblingWindowOperator::TumblingWindowOperator(std::unique_ptr<Function> function, timestamp_t size)
    : WindowOperator(std::move(function)), window_size_(size) {
    // Validate window size
    if (window_size_ <= 0) {
        throw std::invalid_argument("Window size must be positive");
    }
}

std::vector<std::shared_ptr<Window>> TumblingWindowOperator::assignWindows(const DataElement& element) const {
    std::vector<std::shared_ptr<Window>> windows;
    
    // Extract timestamp from element
    timestamp_t timestamp = 0;
    if (element.isRecord() && element.getRecord()) {
        timestamp = element.getEventTime();
    } else {
        // Can't assign windows without a timestamp
        return windows;
    }
    
    // Calculate which tumbling window this element belongs to
    timestamp_t window_start = (timestamp / window_size_) * window_size_;
    timestamp_t window_end = window_start + window_size_;
    
    // Create the window
    auto window = std::make_shared<TumblingWindow>(window_start, window_end, window_size_);
    windows.push_back(window);
    
    return windows;
}

void TumblingWindowOperator::triggerWindow(const std::shared_ptr<Window>& window) {
    // Find the buffer for this window
    auto it = window_buffers_.find(window);
    if (it == window_buffers_.end()) {
        spdlog::warn("No buffer found for window [{}, {})",
                    window->getStart(), window->getEnd());
        return;
    }
    
    const auto& buffer = it->second;
    
    // Process the window using the assigned function
    if (buffer.size() == 0) {
        spdlog::debug("Empty window triggered: [{}, {})",
                     window->getStart(), window->getEnd());
        return;
    }
    
    spdlog::debug("Triggering window [{}, {}) with {} elements",
                 window->getStart(), window->getEnd(), buffer.size());
    
    // Apply the function to the window contents
    // For now this is a placeholder implementation
    // TODO: Implement proper window function application using WindowFunction
    
    // We'll need to convert our buffer of DataElement objects to a vector of DataElement
    // that can be passed to a WindowFunction
    std::vector<DataElement> windowElements;
    for (const auto& element : buffer) {
        // Create a copy of the vector record instead of trying to wrap a reference
        std::unique_ptr<VectorRecord> record_copy = std::make_unique<VectorRecord>(*element.getRecord());
        windowElements.push_back(DataElement(std::move(record_copy)));
    }
    
    // Apply window function if available
    auto* windowFunc = dynamic_cast<WindowFunction*>(getFunction().get());
    DataElement result;
    if (windowFunc) {
        result = windowFunc->process(windowElements, window->getStart(), window->getEnd());
    }
    
    // Emit the result
    for (size_t i = 0; i < getChildren().size(); ++i) {
        emit(i, result);
    }
}

// SlidingWindowOperator implementation

SlidingWindowOperator::SlidingWindowOperator(std::unique_ptr<Function> function, 
                                          timestamp_t size, timestamp_t slide)
    : WindowOperator(std::move(function)), window_size_(size), window_slide_(slide) {
    // Validate window parameters
    if (window_size_ <= 0) {
        throw std::invalid_argument("Window size must be positive");
    }
    
    if (window_slide_ <= 0) {
        throw std::invalid_argument("Window slide must be positive");
    }
}

std::vector<std::shared_ptr<Window>> SlidingWindowOperator::assignWindows(const DataElement& element) const {
    std::vector<std::shared_ptr<Window>> windows;
    
    // Extract timestamp from element
    timestamp_t timestamp = 0;
    if (element.isRecord() && element.getRecord()) {
        timestamp = element.getEventTime();
    } else {
        // Can't assign windows without a timestamp
        return windows;
    }
    
    // Calculate the earliest window start that could contain this element
    timestamp_t earliest_window_start = timestamp - window_size_ + 1;
    
    // Align to window slide boundary (floor to nearest slide)
    earliest_window_start = (earliest_window_start / window_slide_) * window_slide_;
    
    // Create all windows that this element belongs to
    for (timestamp_t window_start = earliest_window_start; 
         window_start <= timestamp; 
         window_start += window_slide_) {
        timestamp_t window_end = window_start + window_size_;
        auto window = std::make_shared<SlidingWindow>(
            window_start, window_end, window_size_, window_slide_);
        windows.push_back(window);
    }
    
    return windows;
}

void SlidingWindowOperator::triggerWindow(const std::shared_ptr<Window>& window) {
    // Similar implementation as TumblingWindowOperator::triggerWindow
    // Find the buffer for this window
    auto it = window_buffers_.find(window);
    if (it == window_buffers_.end()) {
        spdlog::warn("No buffer found for sliding window [{}, {})",
                    window->getStart(), window->getEnd());
        return;
    }
    
    const auto& buffer = it->second;
    
    // Process the window using the assigned function
    if (buffer.size() == 0) {
        spdlog::debug("Empty sliding window triggered: [{}, {})",
                     window->getStart(), window->getEnd());
        return;
    }
    
    spdlog::debug("Triggering sliding window [{}, {}) with {} elements",
                 window->getStart(), window->getEnd(), buffer.size());
    
    // Apply the function to the window contents
    // For now this is a placeholder implementation
    // TODO: Implement proper window function application using WindowFunction
    
    // We'll need to convert our buffer of DataElement objects to a vector of DataElement
    // that can be passed to a WindowFunction
    std::vector<DataElement> windowElements;
    for (const auto& element : buffer) {
        // Create a copy of the vector record instead of trying to wrap a reference
        std::unique_ptr<VectorRecord> record_copy = std::make_unique<VectorRecord>(*element.getRecord());
        windowElements.push_back(DataElement(std::move(record_copy)));
    }
    
    // Apply window function if available
    auto* windowFunc = dynamic_cast<WindowFunction*>(getFunction().get());
    DataElement result;
    if (windowFunc) {
        result = windowFunc->process(windowElements, window->getStart(), window->getEnd());
    }
    
    // Emit the result
    for (size_t i = 0; i < getChildren().size(); ++i) {
        emit(i, result);
    }
}

} // namespace candy