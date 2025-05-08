#include "operator/topk_operator.h"

candy::TopkOperator::TopkOperator(std::unique_ptr<Function>& topk_func,
                                  const std::shared_ptr<ConcurrencyManager>& concurrency_manager)
    : Operator(OperatorType::TOPK), topk_func_(std::move(topk_func)), concurrency_manager_(concurrency_manager) {}

auto candy::TopkOperator::process(Response& data, int slot) -> bool {
  auto topk = dynamic_cast<TopkFunction*>(topk_func_.get());
  if (data.type_ == ResponseType::Record) {
    auto record = std::move(data.record_);
    //std::cout << "topk: " << record->uid_ << '\n';
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