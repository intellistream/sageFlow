//
// Created by Pygon on 25-3-14.
//

#include "query/optimizer/planner.h"

#include "function/function_api.h"
#include "function/window_function.h"
#include "operator/aggregate_operator.h"
#include "operator/itopk_operator.h"
#include "operator/window_operator.h"

namespace candy {

Planner::Planner(const std::shared_ptr<ConcurrencyManager>& concurrency_manager)
    : concurrency_manager_(concurrency_manager) {}

void Planner::planToExecutionGraph(const std::shared_ptr<Stream>& stream,
                                   ExecutionGraph* execution_graph,
                                   size_t default_parallelism) const {
  if (execution_graph == nullptr) {
    throw std::runtime_error("ExecutionGraph is null");
  }

  // 递归构建算子链并添加到执行图（slot 已由 StreamEnvironment 预分配在 Stream 上）
  (void)default_parallelism; // 当前默认并行度策略由 Stream 决定
    buildOperatorChain(stream, execution_graph, default_parallelism, /*inherited_slot_id*/ stream->getSlotId());
}

std::shared_ptr<Operator> Planner::buildOperatorChain(const std::shared_ptr<Stream>& stream,
                                                      ExecutionGraph* execution_graph,
                                                      size_t default_parallelism,
                                                      int inherited_slot_id) const {
  // 若该流已构建，直接返回其尾算子，避免重复
  auto it_cache = built_chains_.find(stream.get());
  if (it_cache != built_chains_.end()) {
    return it_cache->second.tail;
  }

  std::shared_ptr<Operator> head; // 链路头
  std::shared_ptr<Operator> tail; // 链路尾
  std::shared_ptr<Operator> op;   // 当前节点算子

  if (stream->function_ == nullptr) {
    // 数据源算子
    auto source = std::dynamic_pointer_cast<DataStreamSource>(stream);
    head = op = std::make_shared<OutputOperator>(source);
    configureOperatorParallelism(op, stream->getParallelism() > 0 ? stream->getParallelism() : 1);
  } else {
    // 根据函数类型创建相应的算子
    switch (stream->function_->getType()) {
      case FunctionType::Filter:
        head = op = std::make_shared<FilterOperator>(stream->function_);
        configureOperatorParallelism(op, stream->getParallelism());
        break;
      case FunctionType::Map:
        head = op = std::make_shared<MapOperator>(stream->function_);
        configureOperatorParallelism(op, stream->getParallelism());
        break;
      case FunctionType::Join: {
        const auto func = stream->function_.get();
        const auto join_func = dynamic_cast<JoinFunction*>(func);
        const auto& other_stream = join_func->getOtherStream();

        // 递归构建右侧输入流（使用其自身的 slotId）
        auto right_op = buildOperatorChain(other_stream, execution_graph, default_parallelism, /*inherit*/ other_stream->getSlotId());

    // 创建 Join 算子（直接使用 Join Stream 上配置的参数）
    head = op = std::make_shared<JoinOperator>(
      stream->function_,
      concurrency_manager_,
      stream->getJoinMethod(),
      stream->getJoinSimilarityThreshold());
        configureOperatorParallelism(op, stream->getParallelism());

  // 设置母节点（右侧输入）与左右 slot
  const auto join_op = std::dynamic_pointer_cast<JoinOperator>(op);
  join_op->setMother(right_op);
  // 左支承袭当前 stream 的 slot；右支使用 other_stream 的 slot
  join_op->setSlots(/*left*/ (stream->getSlotId() != -1 ? stream->getSlotId() : inherited_slot_id),
        /*right*/ other_stream->getSlotId());

        // 将右侧输入连接到 Join 算子，slot 使用右侧支路的 slotId
        execution_graph->connectOperators(right_op, op, other_stream->getSlotId());
        break;
      }
      case FunctionType::Sink:
        head = op = std::make_shared<SinkOperator>(stream->function_);
        configureOperatorParallelism(op, stream->getParallelism());
        break;
      case FunctionType::Topk:
        head = op = std::make_shared<TopkOperator>(stream->function_, concurrency_manager_);
        configureOperatorParallelism(op, stream->getParallelism());
        break;
      case FunctionType::Window: {
        auto window_func = dynamic_cast<WindowFunction*>(stream->function_.get());
        if (window_func->getWindowType() == WindowType::Tumbling) {
          head = op = std::make_shared<TumblingWindowOperator>(stream->function_);
        } else if (window_func->getWindowType() == WindowType::Sliding) {
          head = op = std::make_shared<SlidingWindowOperator>(stream->function_);
        } else {
          throw std::runtime_error("Unsupported window type");
        }
        configureOperatorParallelism(op, stream->getParallelism());
        break;
      }
      case FunctionType::ITopk:
        head = op = std::make_shared<ITopkOperator>(stream->function_, concurrency_manager_);
        configureOperatorParallelism(op, stream->getParallelism());
        break;
      case FunctionType::Aggregate:
        head = op = std::make_shared<AggregateOperator>(stream->function_);
        configureOperatorParallelism(op, stream->getParallelism());
        break;
      default:
        throw std::runtime_error("Unsupported function type in planner");
    }
  }

  // 将算子添加到执行图
  execution_graph->addOperator(op);
  tail = op;

  // 当前支路使用的 slot：优先 stream 自己的 slotId，否则沿用 inherited_slot_id
  const int current_slot = (stream->getSlotId() != -1) ? stream->getSlotId() : inherited_slot_id;

  // 处理子流依赖（左侧链）：子节点继承当前支路的 slot，并按该 slot 连接
  for (const auto& child_stream : stream->streams_) {
    auto child_tail = buildOperatorChain(child_stream, execution_graph, default_parallelism, /*inherit*/ current_slot);
    // 连接到子链的头算子
    auto it_child = built_chains_.find(child_stream.get());
    auto child_head = (it_child != built_chains_.end()) ? it_child->second.head : child_tail;
    execution_graph->connectOperators(tail, child_head, current_slot);
    tail = child_tail; // 向后推进尾巴
  }

  // 缓存该流构建结果
  built_chains_[stream.get()] = BuiltChainInfo{head, tail};

  return tail;
}

void Planner::configureOperatorParallelism(std::shared_ptr<Operator>& op,
                                           size_t stream_parallelism) const {
  if (op) {
    op->set_parallelism(stream_parallelism);
  }
}

} // namespace candy
