#include "function/topk_function.h"

#include <utility>

candy::TopkFunction::TopkFunction(const std::string& name) : Function(name, FunctionType::Topk) {}

candy::TopkFunction::TopkFunction(const std::string& name, int k, int index_id)
    : Function(name, FunctionType::Topk), k_(k), index_id_(index_id) {}

auto candy::TopkFunction::getK() const -> int { return k_; }

auto candy::TopkFunction::getIndexId() const -> int { return index_id_; }
