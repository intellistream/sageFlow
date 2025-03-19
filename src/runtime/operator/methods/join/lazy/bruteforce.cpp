#include "runtime/operator/methods/join/lazy/bruteforce.h"

namespace candy {
    void BruteForceLazy :: Excute(std :: vector<std :: pair<int, std::unique_ptr<VectorRecord>>> &emit_pool, std::unique_ptr<candy::JoinFunction> &joinfuc, std::list<std::unique_ptr<VectorRecord>> &left_records_, std::list<std::unique_ptr<VectorRecord>> &right_records_) {
        for (auto &left : left_records_)
            for (auto &right : right_records_) {
                auto result = joinfuc -> Execute(left, right);
                if (result != NULL)
                    emit_pool.emplace_back (0, std :: move(result));
            }
    }
    void BruteForceLazy :: Excute(std :: vector<std :: pair<int, std::unique_ptr<VectorRecord>>> &emit_pool, std::unique_ptr<candy::JoinFunction> &joinfuc, std::unique_ptr<VectorRecord> &data, std::list<std::unique_ptr<VectorRecord>> &records_, int slot) {

    }
}