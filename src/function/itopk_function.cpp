//
// Created by Pygon on 25-5-1.
//
#include "function/itopk_function.h"

sageFlow::ITopkFunction::ITopkFunction(const std::string& name) : Function(name, FunctionType::Topk) {}

sageFlow::ITopkFunction::ITopkFunction(const std::string& name, int k, int dim, std::unique_ptr<VectorRecord> record)
    : Function(name, FunctionType::ITopk), k_(k), dim_(dim), record_(std::move(record)) {}

auto sageFlow::ITopkFunction::getK() const -> int { return k_; }

auto sageFlow::ITopkFunction::getDim() const -> int { return dim_; }

auto sageFlow::ITopkFunction::getRecord()-> std::unique_ptr<VectorRecord> { return std::move(record_); }