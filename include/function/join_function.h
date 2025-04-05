#pragma once
#include <functional>

#include "function/function.h"
#include "stream/stream.h"

namespace candy {
using JoinFunc = std::function<bool(std::unique_ptr<VectorRecord> &, std::unique_ptr<VectorRecord> &)>;

class JoinFunction final : public Function {
 public:
  explicit JoinFunction(std::string name);

  JoinFunction(std::string name, JoinFunc join_func);

  auto Execute(std::unique_ptr<VectorRecord> &left,
               std::unique_ptr<VectorRecord> &right) -> std::unique_ptr<VectorRecord> override;

  auto setJoinFunc(JoinFunc join_func) -> void;

  auto getOtherStream() -> std::shared_ptr<Stream> &;

  auto setOtherStream(std::shared_ptr<Stream> other_plan) -> void;

 private:
  JoinFunc join_func_;
  std::shared_ptr<Stream> other_stream_ = nullptr;
};
};  // namespace candy