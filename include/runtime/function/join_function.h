#pragma once
#include <streaming/stream.h>

#include "runtime/function/function.h"

namespace candy {
using JoinFunc = std::function<bool(std::unique_ptr<VectorRecord> &, std::unique_ptr<VectorRecord> &)>;

class JoinFunction : public Function {
 public:
  explicit JoinFunction(std::string name) : Function(std::move(name), FunctionType::Join) {}

  JoinFunction(std::string name, JoinFunc join_func)
      : Function(std::move(name), FunctionType::Join), join_func_(std::move(join_func)) {}

  auto Execute(std::unique_ptr<VectorRecord> &left, std::unique_ptr<VectorRecord> &right)
      -> std::unique_ptr<VectorRecord> override {
    if (join_func_(left, right)) {
      return std::move(left);
    }
    return nullptr;
  }

  auto setJoinFunc(JoinFunc join_func) -> void { join_func_ = std::move(join_func); }

  auto getOtherStream() -> std::shared_ptr<Stream> & { return other_stream_; }

  auto setOtherStream(std::shared_ptr<Stream> other_plan) -> void { other_stream_ = std::move(other_plan); }

 private:
  JoinFunc join_func_;
  std::shared_ptr<Stream> other_stream_ = nullptr;
};
};  // namespace candy