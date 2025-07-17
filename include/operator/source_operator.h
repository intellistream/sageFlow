#pragma once

#include <memory>
#include <thread>
#include <atomic>

#include "operator/operator.h"
#include "function/source_function.h"
#include "common/data_types.h"

namespace candy {

class SourceOperator : public Operator {
 public:
  explicit SourceOperator(std::unique_ptr<SourceFunction> source_func)
    : Operator(OperatorType::SOURCE), source_func_(std::move(source_func)) {}

  ~SourceOperator() override {
    close();
  }

  void open() override;
  void close() override;
  auto process(Response& record, int slot) -> bool override;

  // Source operators don't process input records, they generate data
  void start();
  void stop();

 private:
  std::unique_ptr<SourceFunction> source_func_;
  std::thread worker_thread_;
  std::atomic<bool> is_running_{false};
  
  void runSource();
};

}  // namespace candy
