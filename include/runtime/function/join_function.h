#pragma once
#include <streaming/stream.h>

#include "runtime/function/function.h"

namespace candy {
using JoinFunc = std::function<std::unique_ptr<VectorRecord>(std::unique_ptr<VectorRecord> &, std::unique_ptr<VectorRecord> &)>;

class JoinFunction final : public Function {
 public:
  explicit JoinFunction(std::string name);

  JoinFunction(std::string name, JoinFunc join_func, int64_t time_window);

  auto Execute(std::unique_ptr<VectorRecord> &left, std::unique_ptr<VectorRecord> &right)
      -> std::unique_ptr<VectorRecord> override;

  auto setJoinFunc(JoinFunc join_func) -> void;

  auto getOtherStream() -> std::shared_ptr<Stream> &;

  auto setOtherStream(std::shared_ptr<Stream> other_plan) -> void;

  auto setTimeWindow(int64_t time_window) -> void;

  auto getTimeWindow() -> int64_t;

 private:
  JoinFunc join_func_;
  std::shared_ptr<Stream> other_stream_ = nullptr;
  int64_t time_window_;
};
};  // namespace candy