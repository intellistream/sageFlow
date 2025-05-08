#include "operator/topk_operator.h"

candy::TopkOperator::TopkOperator(std::unique_ptr<Function>&& topk_func,
                                  ConcurrencyManager& concurrency_manager)
    : Operator(OperatorType::TOPK), topk_func_(std::move(topk_func)), concurrency_manager_(concurrency_manager) {}

auto candy::TopkOperator::process(DataElement& element, int slot) -> bool {
  auto topk = dynamic_cast<TopKFunction*>(topk_func_.get());
  if (element.isRecord()) {
    auto record = element.getRecord();
    std::cout << "topK: " << record->uid_ << '\n';
    auto index_id = topk->getIndexId();
    auto k = topk->getK();
    
    // Create a unique_ptr with a copy of the record for querying
    auto recordCopy = std::make_unique<VectorRecord>(*record);
    auto res = concurrency_manager_.query(index_id, recordCopy, k);
    
    auto records = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>(std::move(res));
    auto result = DataElement(std::move(records));
    emit(0, result);
    return true;
  }
  return false;
}