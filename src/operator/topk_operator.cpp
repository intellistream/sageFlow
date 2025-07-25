#include "operator/topk_operator.h"

#include <mutex>

candy::TopkOperator::TopkOperator(std::unique_ptr<Function>& topk_func,
                                  const std::shared_ptr<ConcurrencyManager>& concurrency_manager)
    : Operator(OperatorType::TOPK), topk_func_(std::move(topk_func)), concurrency_manager_(concurrency_manager) {}

auto candy::TopkOperator::process(Response& data, int slot) -> bool {
  // TODO: 多线程改造 - TopK算子的并发安全
  // 在多线程环境中，需要考虑以下改造：
  // 1. 索引的并发访问保护（concurrency_manager已经处理）
  // 2. 结果聚合：多个线程可能产生不同的TopK结果，需要合并
  // 3. 状态同步：如果需要维护全局TopK状态
  // 4. 分区TopK：每个线程维护局部TopK，定期合并全局结果

  auto topk = dynamic_cast<TopkFunction*>(topk_func_.get());
  if (data.type_ == ResponseType::Record) {
    auto record = std::move(data.record_);

    // 当前实现依赖ConcurrencyManager的线程安全查询
    auto index_id = topk->getIndexId();
    auto k = topk->getK();
    auto res = concurrency_manager_->query(index_id, record, k);
    auto records = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>(std::move(res));
    auto resp = Response{ResponseType::List, std::move(records)};
    emit(0, resp);
    return true;
  }
  return false;
}