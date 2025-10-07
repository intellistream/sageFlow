//
// Created by Pygon on 25-5-7.
//
#include "function/aggregate_function.h"

sageFlow::AggregateFunction::AggregateFunction(const std::string& name) : Function(name, FunctionType::None) {}

sageFlow::AggregateFunction::AggregateFunction(const std::string& name, AggregateType aggregate_type)
    : Function(name, FunctionType::Aggregate), aggregate_type_(aggregate_type) {}

auto sageFlow::AggregateFunction::getAggregateType() const -> AggregateType { return aggregate_type_; }