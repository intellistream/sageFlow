#pragma once

#include <memory>

#include "runtime/function/function.h"
#include "runtime/function/join_function.h"
#include "runtime/operator/base_operator.h"
#include "runtime/operator/filter_operator.h"
#include "runtime/operator/join_operator.h"
#include "runtime/operator/map_operator.h"
#include "runtime/operator/output_operator.h"
#include "runtime/operator/sink_operator.h"

namespace candy {

class Planner {
 public:
  Planner() = default;

  auto plan(const std::shared_ptr<Stream>& stream) const -> std::shared_ptr<Operator> {
    std::shared_ptr<Operator> op = nullptr;
    if (stream->function_ == nullptr) {
      // turn stream into stream source
      auto source = std::dynamic_pointer_cast<DataStreamSource>(stream);
      op = std::make_shared<OutputOperator>(source);
    } else {
      if (stream->function_->getType() == FunctionType::Filter) {
        op = std::make_shared<FilterOperator>(stream->function_);
      } else if (stream->function_->getType() == FunctionType::Map) {
        op = std::make_shared<MapOperator>(stream->function_);
      } else if (stream->function_->getType() == FunctionType::Join) {
        const auto func = stream->function_.get();
        const auto join_func = dynamic_cast<JoinFunction*>(func);
        const auto& other_stream = join_func->getOtherStream();
        const auto other_op = plan(other_stream);
        op = std::make_shared<JoinOperator>(stream->function_);
        const auto join_op = std::dynamic_pointer_cast<JoinOperator>(op);
        join_op->setMother(other_op);
        other_op->addChild(op, 1);
      } else if (stream->function_->getType() == FunctionType::Sink) {
        op = std::make_shared<SinkOperator>(stream->function_);
      }
    }
    for (auto& child : stream->streams_) {
      op->addChild(plan(child));
    }
    return op;
  }
};
}  // namespace candy