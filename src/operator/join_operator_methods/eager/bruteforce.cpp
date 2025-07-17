#include "operator/join_operator_methods/eager/bruteforce.h"

namespace candy {
    void BruteForceEager :: Excute(std :: vector<std :: pair<int, std::unique_ptr<VectorRecord>>> &emit_pool, std::unique_ptr<candy::JoinFunction> &joinfuc, std::list<std::unique_ptr<VectorRecord>> &left_records, std::list<std::unique_ptr<VectorRecord>> &right_records) {
        
    }
    void BruteForceEager :: Excute(std :: vector<std :: pair<int, std::unique_ptr<VectorRecord>>> &emit_pool, std::unique_ptr<candy::JoinFunction> &joinfuc, std::unique_ptr<VectorRecord> &data, std::list<std::unique_ptr<VectorRecord>> &records, int slot) {
        for (auto &rec : records) {
            Response response_rec;
            response_rec.push_back(std::move(rec));
            Response response_data;
            response_data.push_back(std::move(data));
            auto ret = joinfuc -> Execute(response_rec, response_data);
            if (!ret.empty() && ret[0] != nullptr) {
                emit_pool.emplace_back(0, std :: move(ret[0]));
            }
        }
    }
} // namespace candy