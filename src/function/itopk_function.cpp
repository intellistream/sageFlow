//
// Created by Pygon on 25-5-1.
//
#include "function/itopk_function.h"

candy::ITopkFunction::ITopkFunction(const std::string& name) : Function(name, FunctionType::Topk) {}

candy::ITopkFunction::ITopkFunction(const std::string& name, int k) : Function(name, FunctionType::Topk), k_(k) {}

auto candy::ITopkFunction::getK() const -> int {
  return k_;
}