#include "operator/topk_operator.h"

candy::TopkOperator::TopkOperator(std::unique_ptr<Function>& topk_func,
                                  const std::shared_ptr<ConcurrencyManager>& concurrency_manager)
    : Operator(OperatorType::TOPK), topk_func_(std::move(topk_func)), concurrency_manager_(concurrency_manager) {}

auto candy::TopkOperator::process(Response& data, int slot) -> bool {
  auto topk = dynamic_cast<TopkFunction*>(topk_func_.get());
  
  for (auto& record : data) {
    if (record) {
      //std::cout << "topk: " << record->uid_ << '\n';
      auto index_id = topk->getIndexId();
      auto k = topk->getK();
      auto res = concurrency_manager_->query(index_id, record, k);
      
      // Create response from query results
      Response resp;
      for (auto& result_record : res) {
        resp.push_back(std::move(result_record));
      }
      
      emit(0, resp);
    }
  }
  
  return !data.empty();
}