#pragma once

#include "function/function.h"

namespace candy {
enum class WindowType {
  Sliding,
  Tumbling
};

class WindowFunction final : public Function {
 public:
  explicit WindowFunction(std::string name);
  WindowFunction(std::string name, int window_size, int slide_size, WindowType window_type);

  auto Execute(Response &resp) -> Response override;

  auto getWindowType() const -> WindowType;
  auto getWindowSize() const -> int;
  auto getSlideSize() const -> int;

 private:
  WindowType window_type_;
  int window_size_;
  int slide_size_;
};
};  // namespace candy