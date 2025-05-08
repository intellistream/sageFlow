#include "operator/join_operator_methods/lazy/bruteforce.h"

namespace candy {
    void BruteForceLazy :: Excute(std :: vector<std :: pair<int, std::unique_ptr<VectorRecord>>> &emit_pool, std::unique_ptr<candy::JoinFunction> &joinfuc, std::list<std::unique_ptr<VectorRecord>> &left_records_, std::list<std::unique_ptr<VectorRecord>> &right_records_) {
        for (auto &left : left_records_)
            for (auto &right : right_records_) {
                // Create deep copies since we need to retain the originals
                std::unique_ptr<VectorRecord> left_copy = std::make_unique<VectorRecord>(*left);
                std::unique_ptr<VectorRecord> right_copy = std::make_unique<VectorRecord>(*right);
                
                auto element_left = DataElement(std::move(left_copy));
                auto element_right = DataElement(std::move(right_copy));
                
                auto result = joinfuc->Execute(element_left, element_right);
                
                if (result.isRecord() && result.getRecord() != nullptr) {
                    std::unique_ptr<VectorRecord> result_record = std::make_unique<VectorRecord>(*result.getRecord());
                    emit_pool.emplace_back(0, std::move(result_record));
                }
            }
        
    }
    void BruteForceLazy :: Excute(std :: vector<std :: pair<int, std::unique_ptr<VectorRecord>>> &emit_pool, std::unique_ptr<candy::JoinFunction> &joinfuc, std::unique_ptr<VectorRecord> &data, std::list<std::unique_ptr<VectorRecord>> &records_, int slot) {

    }
}