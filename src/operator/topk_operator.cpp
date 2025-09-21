#include "operator/topk_operator.h"

#include <mutex>

candy::TopkOperator::TopkOperator(std::unique_ptr<Function>& topk_func,
                                  const std::shared_ptr<ConcurrencyManager>& concurrency_manager)
    : Operator(OperatorType::TOPK), topk_func_(std::move(topk_func)), concurrency_manager_(concurrency_manager) {}

auto candy::TopkOperator::process(Response&data, int slot) -> std::optional<Response> {
  // TODO: 多线程改造 - TopK算子的并发安全
  // 在多线程环境中，需要考虑以下改造：
  // 1. 索引的并发访问保护（concurrency_manager已经处理）
  // 2. 结果聚合：多个线程可能产生不同的TopK结果，需要合并
  // 3. 状态同步：如果需要维护全局TopK状态
  // 4. 分区TopK：每个线程维护局部TopK，定期合并全局结果

  auto topk = dynamic_cast<TopkFunction*>(topk_func_.get());
  if (data.type_ == ResponseType::Record) {
    auto record = data.record_.get(); // Use const reference

    // 当前实现依赖ConcurrencyManager的线程安全查询
    auto index_id = topk->getIndexId();
    auto k = topk->getK();
    // Use const reference for query
    auto res = concurrency_manager_->query(index_id, *record, k);

    // Convert shared_ptr<const VectorRecord> to unique_ptr<VectorRecord>
    auto records = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>();
    records->reserve(res.size());
    for (const auto& shared_record : res) {
      // Create a copy since we need unique_ptr and the original is const
      records->emplace_back(std::make_unique<VectorRecord>(*shared_record));
    }

    auto response = Response(ResponseType::List, std::move(records));
    return response;
  }
  return std::nullopt;
}

auto candy::TopkOperator::apply(Response&& record, int slot, Collector& collector) -> void {
  auto topk = dynamic_cast<TopkFunction*>(topk_func_.get());
  if (record.type_ == ResponseType::Record && record.record_) {
    // 使用ConcurrencyManager进行线程安全的TopK查询
    auto index_id = topk->getIndexId();
    auto k = topk->getK();
    auto res = concurrency_manager_->query(index_id, *record.record_, k);

    // 将查询结果转换为Response格式
    auto records = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>();
    records->reserve(res.size());
    for (const auto& shared_record : res) {
      records->emplace_back(std::make_unique<VectorRecord>(*shared_record));
    }

    Response response(ResponseType::List, std::move(records));
    collector.collect(std::make_unique<Response>(std::move(response)), slot);
  }
}
