#pragma once
#include <functional>

#include "function/function.h"

namespace sageFlow {
using SinkFunc = std::function<void(std::unique_ptr<VectorRecord> &)>;
// Pair-aware sink callback for PAIR_PASSTHROUGH results. Receives read-only
// shared views of the two matched records plus the similarity score, so a
// downstream LLM pre-processing task can read both original payloads zero-copy.
using PairSinkFunc =
    std::function<void(const RecordView &left, const RecordView &right, double similarity)>;

class SinkFunction final : public Function {
 public:
  explicit SinkFunction(std::string name);

  SinkFunction(std::string name, SinkFunc sink_func);

  auto Execute(Response &resp) -> Response override;

  auto setSinkFunc(SinkFunc sink_func) -> void;

  auto setPairSinkFunc(PairSinkFunc pair_sink_func) -> void;

 private:
  SinkFunc sink_func_;
  PairSinkFunc pair_sink_func_;
};
};  // namespace sageFlow