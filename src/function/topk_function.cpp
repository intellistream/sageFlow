#include "function/topk_function.h"

candy::TopkFunction::TopkFunction(const std::string& name) : Function(name, FunctionType::Topk) {}

candy::TopkFunction::TopkFunction(const std::string& name, const TopkFunc& topk_func)
    : Function(name, FunctionType::Topk), topk_func_(topk_func) {}

candy::Response candy::TopkFunction::Execute(Response& resp) { return Function::Execute(resp); }