//
// Created by Pygon on 25-5-1.
//
#include "operator/itopk_operator.h"

candy::ITopkOperator::ITopkOperator(std::unique_ptr<Function>& itopk_func) : Operator(OperatorType::ITOPK) {
  itopk_func_ = std::move(itopk_func);
}

bool candy::ITopkOperator::process(Response& data, const int slot) { return Operator::process(data, slot); }