#include "runtime/operator/methods/join/eager/bruteforce.h"

namespace candy {
    void BruteForceEager :: Excute(std :: vector<std :: pair<int, std::unique_ptr<VectorRecord>>> &emit_pool, std::unique_ptr<candy::JoinFunction> &joinfuc, std::list<std::unique_ptr<VectorRecord>> &left_records_, std::list<std::unique_ptr<VectorRecord>> &right_records_) {
        
    }
    void BruteForceEager :: Excute(std :: vector<std :: pair<int, std::unique_ptr<VectorRecord>>> &emit_pool, std::unique_ptr<candy::JoinFunction> &joinfuc, std::unique_ptr<VectorRecord> &data, std::list<std::unique_ptr<VectorRecord>> &records_, int slot) {
        for (auto &rec : records_) {
            auto ret = joinfuc -> Execute(rec, data);
            if (ret != NULL)
                emit_pool.emplace_back(0, std :: move(ret));
        }
    }
}