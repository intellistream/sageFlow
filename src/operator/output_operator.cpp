#include "operator/output_operator.h"

candy::OutputOperator::OutputOperator() : Operator(OperatorType::OUTPUT) {}

candy::OutputOperator::OutputOperator(const OutputChoice output_choice, std::shared_ptr<DataStreamSource> stream)
    : Operator(OperatorType::OUTPUT), output_choice_(output_choice), stream_(std::move(stream)) {}

candy::OutputOperator::OutputOperator(std::shared_ptr<DataStreamSource> stream)
    : Operator(OperatorType::OUTPUT), stream_(std::move(stream)) {}

auto candy::OutputOperator::open() -> void {
  if (is_open_) {
    return;
  }
  is_open_ = true;

  // TODO: 多线程改造 - 数据源的并发读取
  // 在多线程环境中，需要考虑以下改造：
  // 1. 数据源的分区读取，每个ExecutionVertex读取不同的数据分区
  // 2. 使用线程安全的数据生成器
  // 3. 控制数据生成的速率，避免内存溢出

  if (stream_) {
    stream_->Init();
  }

}

auto candy::OutputOperator::process(Response&data, int slot) -> std::optional<Response> {
  // OutputOperator作为数据源，通常不需要处理输入数据
  // 而是负责从数据流中读取数据并发射到下游
  // 在新的collector模式下，OutputOperator主要用于数据生成

  // 注意：在新的多线程模型中，数据读取应该在ExecutionVertex的run()方法中进行
  // 这里暂时保留原有逻辑，后续需要重构为流式处理

//   // TODO: 多线程改造 - 输出分发策略优化
//   // 当前的分发策略需要在ExecutionGraph中通过ResultPartition实现
//   // 这里的逻辑主要用于兼容性
//
//   if (output_choice_ == OutputChoice::Broadcast) {
//     // 广播模式：复制数据到所有下游
//     for (size_t i = 0; i < children_.size(); i++) {
//       auto copy = data;
//       emit(i, copy);
//     }
//   } else if (output_choice_ == OutputChoice::Hash) {
//     // 哈希分区模式：根据数据特征分发
//     // TODO: 实现更智能的哈希分区逻辑
//     auto id = 0;  // 暂时使用简单的轮询
//     if (!children_.empty()) {
//       children_[id]->process(data, 0);
//     }
//   } else if (output_choice_ == OutputChoice::NONE) {
//     // 默认模式：发送到第一个下游
//     emit(0, data);
//   }
   return std::nullopt;
}

auto candy::OutputOperator::run(Collector& collector) -> void {
  std::unique_ptr<VectorRecord> record = nullptr;
  while (stream_ && (record = stream_->Next())) {
    auto resp = Response{ResponseType::Record, std::move(record)};
  // 使用广播到当前顶点配置的所有 slot，避免硬编码 slot=0
  apply(std::move(resp), -1, collector);
  }
}

auto candy::OutputOperator::apply(Response&& record, int slot, Collector& collector) -> void {
  if (record.type_ != ResponseType::None) {
    if (output_choice_ == OutputChoice::Broadcast) {
      // 广播模式：将数据发送到所有下游slot
      // 在collector模式下，-1表示广播到所有下游
      collector.collect(std::make_unique<Response>(std::move(record)), -1);
    } else if (output_choice_ == OutputChoice::Hash) {
      // 哈希分区模式：根据数据特征确定slot
      int target_slot = 0;  // 简化的哈希逻辑，实际可以基于record的uid进行哈希
      if (record.record_) {
        target_slot = static_cast<int>(record.record_->uid_ % 4);  // 假设最多4个并行度
      }
      collector.collect(std::make_unique<Response>(std::move(record)), target_slot);
    } else {
      // 默认模式：发送到指定slot
      collector.collect(std::make_unique<Response>(std::move(record)), slot);
    }
  }
}
