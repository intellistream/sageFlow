//
// Created by Pygon on 25-4-30.
//
#include "function/window_function.h"

candy::WindowFunction::WindowFunction(std::string name)
    : Function(std::move(name), FunctionType::Window),
      window_type_(WindowType::Tumbling),
      window_size_(0),
      slide_size_(0) {}

candy::WindowFunction::WindowFunction(std::string name, const int window_size, const int slide_size,
                                      const WindowType window_type)
    : Function(std::move(name), FunctionType::Window),
      window_type_(window_type),
      window_size_(window_size),
      slide_size_(slide_size) {}

auto candy::WindowFunction::Execute(Response& resp) -> candy::Response { return Function::Execute(resp); }

auto candy::WindowFunction::getWindowType() const -> WindowType { return window_type_; }

auto candy::WindowFunction::getWindowSize() const -> int { return window_size_; }

auto candy::WindowFunction::getSlideSize() const -> int { return slide_size_; }