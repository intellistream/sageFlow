#include "operator/join_operator_methods/eager/bruteforce.h"

namespace candy {
    void BruteForceEager :: Excute(std :: vector<std :: pair<int, std::unique_ptr<VectorRecord>>> &emit_pool, std::unique_ptr<candy::JoinFunction> &joinfuc, std::list<std::unique_ptr<VectorRecord>> &left_records_, std::list<std::unique_ptr<VectorRecord>> &right_records_) {
        
    }
    void BruteForceEager :: Excute(std :: vector<std :: pair<int, std::unique_ptr<VectorRecord>>> &emit_pool, std::unique_ptr<candy::JoinFunction> &joinfuc, std::unique_ptr<VectorRecord> &data, std::list<std::unique_ptr<VectorRecord>> &records_, int slot) {
        for (auto &rec : records_) {
            // Create deep copies since we need to retain the originals
            std::unique_ptr<VectorRecord> rec_copy = std::make_unique<VectorRecord>(*rec);
            std::unique_ptr<VectorRecord> data_copy = std::make_unique<VectorRecord>(*data);
            
            auto element_rec = DataElement(std::move(rec_copy));
            auto element_data = DataElement(std::move(data_copy));
            
            auto ret = joinfuc->Execute(element_rec, element_data);
            
            if (ret.isRecord() && ret.getRecord() != nullptr) {
                std::unique_ptr<VectorRecord> ret_record = std::make_unique<VectorRecord>(*ret.getRecord());
                emit_pool.emplace_back(0, std::move(ret_record));
            }
        }
    }
}