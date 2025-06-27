/*
修改 Eager/Lazy 通过process 中的 IsEagerAlgorithm 修改
修改调用的方法， 则修改 using JoinWay 后面的等于号
*/
#include "operator/join_operator.h"
#include "operator/join_operator_methods/join_methods.h"

#include <cassert>
#include <iostream>

#include "spdlog/fmt/bundled/chrono.h"

// using JoinWay = candy :: BruteForceLazy;

namespace candy {

void JoinOperator::initializeIVFIndexes(int nlist, double rebuild_threshold, int nprobes) {
    if (!concurrency_manager_) {
        left_ivf_index_id_ = -1; // Mark as invalid
        right_ivf_index_id_ = -1; // Mark as invalid
        return;
    }
    // Use the IVF-specific create_index overload
    left_ivf_index_id_ = concurrency_manager_->create_index(
      "left_join_ivf", IndexType::IVF, join_func_->getDim()/*, nlist, rebuild_threshold, nprobes*/);
    right_ivf_index_id_ = concurrency_manager_->create_index(
      "right_join_ivf", IndexType::IVF, join_func_->getDim()/*, nlist, rebuild_threshold, nprobes*/);
}

JoinOperator::JoinOperator(std::unique_ptr<Function> &join_func,
                           const std::shared_ptr<ConcurrencyManager> &concurrency_manager,
                           const std::string& join_method_name,
                           double join_similarity_threshold)
    : Operator(OperatorType::JOIN), concurrency_manager_(concurrency_manager),
      join_similarity_threshold_(join_similarity_threshold) {
    join_func_ = std::unique_ptr<JoinFunction>(dynamic_cast<JoinFunction*>(join_func.release()));
    if (join_func_ == nullptr) {
        throw std::runtime_error("JoinOperator: join_func is not a JoinFunction");
    }
    if (!concurrency_manager_) {
        throw std::runtime_error("JoinOperator: concurrency_manager is a nullptr");
    }
    is_eager_  = (join_method_name == "ivf_eager") || (join_method_name == "bruteforce_eager");
    using_ivf_ = (join_method_name == "ivf_eager") || (join_method_name == "ivf_lazy");
    // Default IVF parameters, in the future change to read from the configuration file
    int ivf_nlist = 100;
    int ivf_nprobes = 10;
    double ivf_rebuild_threshold = 0.5;
    if (join_method_name == "ivf_eager") {
        if (concurrency_manager_) {
            initializeIVFIndexes(ivf_nlist, ivf_rebuild_threshold, ivf_nprobes);
            if (left_ivf_index_id_ != -1 && right_ivf_index_id_ != -1) {
                join_method_ = std::make_unique<IvfEager>(left_ivf_index_id_, right_ivf_index_id_,
                                                          join_similarity_threshold, concurrency_manager_);
              using_ivf_ = true;
            } else {
                join_method_ = std::make_unique<BruteForceEager>(join_similarity_threshold);
                using_ivf_ = false;
            }
        } else {
            join_method_ = std::make_unique<BruteForceEager>(join_similarity_threshold);
            using_ivf_ = false;
        }
    } else if (join_method_name == "ivf_lazy") {
        if (concurrency_manager_) {
            initializeIVFIndexes(ivf_nlist, ivf_rebuild_threshold, ivf_nprobes);
            if (left_ivf_index_id_ != -1 && right_ivf_index_id_ != -1) {
                join_method_ = std::make_unique<IvfLazy>(left_ivf_index_id_, right_ivf_index_id_,
                                                         join_similarity_threshold, concurrency_manager_);
                using_ivf_ = true;
            } else {
                join_method_ = std::make_unique<BruteForceLazy>(join_similarity_threshold);
                using_ivf_ = false;
            }
        } else {
            join_method_ = std::make_unique<BruteForceLazy>(join_similarity_threshold);
            using_ivf_ = false;
        }
    } else if (join_method_name == "bruteforce_eager") {
        join_method_ = std::make_unique<BruteForceEager>(join_similarity_threshold);
    } else if (join_method_name == "bruteforce_lazy") {
        join_method_ = std::make_unique<BruteForceLazy>(join_similarity_threshold);
    } else {
        join_method_ = std::make_unique<BruteForceLazy>(join_similarity_threshold);
        using_ivf_ = false;
        is_eager_ = false;
    }
}

void JoinOperator::open() {
  if (is_open_) {
    return;
  }
  is_open_ = true;
  if (mother_) {
    mother_->open();
  }
  for (const auto& child : children_) {
    if (child) {
      child->open();
    }
  }
}

auto JoinOperator::lazy_process(const int slot) -> bool {
  if (left_records_.empty() || right_records_.empty()) {
    return false;
  }

  join_method_->Excute(methods_return_pool_, join_func_, left_records_, right_records_);
  left_records_.clear();
  right_records_.clear();

  return true;
}

auto JoinOperator::eager_process(const int slot) -> bool {
  if (left_records_.empty() && slot == 1) { // Need at least one left record if data comes from right
    return false;
  }
  if (right_records_.empty() && slot == 0) { // Need at least one right record if data comes from left
      return false;
  }
  // Ensure there's a newest record in the arrival slot's list before accessing .back()
  if (slot == 0 && left_records_.empty()) { return false; }
  if (slot == 1 && right_records_.empty()) { return false; }
  if (slot == 0) {
    join_method_->Excute(methods_return_pool_, join_func_, left_records_.back(), right_records_, slot);
  } else {
    join_method_->Excute(methods_return_pool_, join_func_, right_records_.back(), left_records_, slot);
  }
  return true;
}

void JoinOperator::clear_methods_return_pool() {
  for (auto &[id, record] : methods_return_pool_) {
    auto ret = Response{ResponseType::Record, std::move(record)};
    emit (id, ret);
  }
  methods_return_pool_.clear();
}

auto JoinOperator::process(Response& input_data, int slot) -> bool {
    if (!input_data.record_) {
        return false;
    }
    std::unique_ptr<VectorRecord> data_ptr = std::move(input_data.record_); // Renamed to avoid conflict with lambda capture
    uint64_t current_uid = data_ptr->uid_;
    int64_t nowTimeStamp = data_ptr->timestamp_;

    auto update_side = [&](std::list<std::unique_ptr<VectorRecord>>& records, SlidingWindow& window, int index_id_for_cc) -> bool {
        std::unique_ptr<VectorRecord> data_for_cc_insert = nullptr;
        if (using_ivf_ && concurrency_manager_ && index_id_for_cc != -1) { // Check valid index_id
             data_for_cc_insert = std::make_unique<VectorRecord>(*data_ptr); // Use data_ptr
        }

        records.emplace_back(std::move(data_ptr)); // data_ptr is moved here.

        if (using_ivf_ && concurrency_manager_ && data_for_cc_insert && index_id_for_cc != -1) {
            concurrency_manager_->insert(index_id_for_cc, data_for_cc_insert);
        } else if (using_ivf_ && (index_id_for_cc == -1 || !concurrency_manager_)) {
            // Potentially fall back to a non-indexed operation or log an error that data won't be indexed.
        }

        int timelimit = window.windowTimeLimit(nowTimeStamp);
        while (!records.empty() && records.front()->timestamp_ <= timelimit) {
            uint64_t expired_uid = records.front()->uid_;
            records.pop_front();
            if (using_ivf_ && concurrency_manager_ && index_id_for_cc != -1) {
                concurrency_manager_->erase(index_id_for_cc, expired_uid);
            } else if (using_ivf_ && (index_id_for_cc == -1 || !concurrency_manager_)) {
                //TODO
            }
        }
        return window.isNeedTrigger(nowTimeStamp);
    };

    bool trigger_flag;
    if (slot == 0) {
        trigger_flag = update_side(left_records_, join_func_->windowL, left_ivf_index_id_);
    } else {
        trigger_flag = update_side(right_records_, join_func_->windowR, right_ivf_index_id_);
    }

    if (!trigger_flag) {
        return false;
    }

    bool return_flag;
    if (is_eager_) {
        return_flag = eager_process(slot);
    } else {
        return_flag = lazy_process(slot);
    }

    clear_methods_return_pool();
    return return_flag;
}

auto JoinOperator::setMother(std::shared_ptr<Operator> mother) -> void { mother_ = std::move(mother); }

} // namespace candy
