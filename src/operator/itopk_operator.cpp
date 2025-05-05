//
// Created by Pygon on 25-5-1.
//
#include "operator/itopk_operator.h"

candy::ITopkOperator::ITopkOperator(std::unique_ptr<Function>& itopk_func,
                                    const std::shared_ptr<ConcurrencyManager>& concurrency_manager)
    : Operator(OperatorType::ITOPK), itopk_func_(std::move(itopk_func)), concurrency_manager_(concurrency_manager) {}

auto candy::ITopkOperator::process(Response& data, const int slot) -> bool { return Operator::process(data, slot); }