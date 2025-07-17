#include "operator/join_operator_methods/lazy/bruteforce.h"

namespace candy {
    void BruteForceLazy :: Excute(std :: vector<std :: pair<int, std::unique_ptr<VectorRecord>>> &emit_pool, std::unique_ptr<candy::JoinFunction> &joinfuc, std::list<std::unique_ptr<VectorRecord>> &left_records, std::list<std::unique_ptr<VectorRecord>> &right_records) {
        for (auto &left : left_records) {
            for (auto &right : right_records) {
                Response response_left;
                response_left.push_back(std::move(left));
                Response response_right;
                response_right.push_back(std::move(right));
                auto result = joinfuc -> Execute(response_left, response_right);
                if (!result.empty() && result[0] != nullptr) {
                    emit_pool.emplace_back (0, std :: move(result[0]));
                }
            }
        }
    }
    void BruteForceLazy :: Excute(std :: vector<std :: pair<int, std::unique_ptr<VectorRecord>>> &emit_pool, std::unique_ptr<candy::JoinFunction> &joinfuc, std::unique_ptr<VectorRecord> &data, std::list<std::unique_ptr<VectorRecord>> &records, int slot) {

    }
} // namespace candy