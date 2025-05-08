#include "function/topk_function.h"

#include <utility>

candy::TopKFunction::TopKFunction(const std::string& name) : Function(name, FunctionType::Topk) {}

candy::TopKFunction::TopKFunction(const std::string& name, int k, int index_id)
    : Function(name, FunctionType::Topk), k_(k), index_id_(index_id) {}

auto candy::TopKFunction::getK() const -> int { return k_; }

auto candy::TopKFunction::getIndexId() const -> int { return index_id_; }
