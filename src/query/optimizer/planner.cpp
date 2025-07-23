//
// Created by Pygon on 25-3-14.
//

#include "query/optimizer/planner.h"

#include "function/function_api.h"
#include "function/window_function.h"
#include "operator/aggregate_operator.h"
#include "operator/itopk_operator.h"
#include "operator/window_operator.h"

candy::Planner::Planner(const std::shared_ptr<ConcurrencyManager>& concurrency_manager)
    : concurrency_manager_(concurrency_manager) {}

void candy::Planner::planToExecutionGraph(const std::shared_ptr<Stream>& stream,
                                         ExecutionGraph* execution_graph,
                                         size_t default_parallelism) const {
  if (!execution_graph) {
    throw std::runtime_error("ExecutionGraph is null");
  }

  // 递归构建算子链并添加到执行图
  auto root_operator = buildOperatorChain(stream, execution_graph, default_parallelism);

  // 根算子已经在buildOperatorChain中添加到执行图了
}

std::shared_ptr<Operator> candy::Planner::buildOperatorChain(const std::shared_ptr<Stream>& stream,
                                                            ExecutionGraph* execution_graph,
                                                            size_t default_parallelism) const {
  std::shared_ptr<Operator> op = nullptr;

  if (stream->function_ == nullptr) {
    // 数据源算子
    auto source = std::dynamic_pointer_cast<DataStreamSource>(stream);
    op = std::make_shared<OutputOperator>(source);
    configureOperatorParallelism(op, 1);  // 源算子通常并行度为1
  } else {
    // 根据函数类型创建相应的算子
    if (stream->function_->getType() == FunctionType::Filter) {
      op = std::make_shared<FilterOperator>(stream->function_);
      configureOperatorParallelism(op, default_parallelism);
    } else if (stream->function_->getType() == FunctionType::Map) {
      op = std::make_shared<MapOperator>(stream->function_);
      configureOperatorParallelism(op, default_parallelism);
    } else if (stream->function_->getType() == FunctionType::Join) {
      const auto func = stream->function_.get();
      const auto join_func = dynamic_cast<JoinFunction*>(func);
      const auto& other_stream = join_func->getOtherStream();

      // 递归构建另一个输入流
      const auto other_op = buildOperatorChain(other_stream, execution_graph, default_parallelism);

      op = std::make_shared<JoinOperator>(stream->function_, concurrency_manager_);
      configureOperatorParallelism(op, default_parallelism);

      const auto join_op = std::dynamic_pointer_cast<JoinOperator>(op);
      join_op->setMother(other_op);

      // 在执行图中连接另一个输入流到Join算子
      execution_graph->connectOperators(other_op, op, 1);
    } else if (stream->function_->getType() == FunctionType::Sink) {
      op = std::make_shared<SinkOperator>(stream->function_);
      configureOperatorParallelism(op, 1);  // Sink通常并行度为1
    } else if (stream->function_->getType() == FunctionType::Topk) {
      op = std::make_shared<TopkOperator>(stream->function_, concurrency_manager_);
      configureOperatorParallelism(op, default_parallelism);
    } else if (stream->function_->getType() == FunctionType::Window) {
      auto func = stream->function_.get();
      auto window_func = dynamic_cast<WindowFunction*>(func);
      if (window_func->getWindowType() == WindowType::Tumbling) {
        op = std::make_shared<TumblingWindowOperator>(stream->function_);
      } else if (window_func->getWindowType() == WindowType::Sliding) {
        op = std::make_shared<SlidingWindowOperator>(stream->function_);
      } else {
        throw std::runtime_error("Unsupported window type");
      }
      configureOperatorParallelism(op, default_parallelism);
    } else if (stream->function_->getType() == FunctionType::ITopk) {
      op = std::make_shared<ITopkOperator>(stream->function_, concurrency_manager_);
      configureOperatorParallelism(op, default_parallelism);
    } else if (stream->function_->getType() == FunctionType::Aggregate) {
      op = std::make_shared<AggregateOperator>(stream->function_);
      configureOperatorParallelism(op, default_parallelism);
    } else {
      throw std::runtime_error("Unsupported function type in planner");
    }
  }

  // 将算子添加到执行图
  execution_graph->addOperator(op);

  // 处理子流依赖 - 使用streams_成员而不是parent_
  for (const auto& child_stream : stream->streams_) {
    auto child_op = buildOperatorChain(child_stream, execution_graph, default_parallelism);
    execution_graph->connectOperators(op, child_op, 0);
  }

  return op;
}

void candy::Planner::configureOperatorParallelism(std::shared_ptr<Operator>& op,
                                                  size_t default_parallelism) const {
  if (op) {
    // 根据算子类型设置合适的并行度
    switch (op->getType()) {
      case OperatorType::OUTPUT:
        op->set_parallelism(1);  // 源算子通常为1
        break;
      case OperatorType::SINK:
        op->set_parallelism(1);  // Sink算子通常为1
        break;
      case OperatorType::JOIN:
        // Join算子可能需要特殊处理，暂时使用默认并行度
        op->set_parallelism(default_parallelism);
        break;
      default:
        op->set_parallelism(default_parallelism);
        break;
    }
  }
}
