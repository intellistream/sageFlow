#include "index/vectraflow.h"
#include <queue>
#include <omp.h>

candy::VectraFlow::~VectraFlow() = default;

auto candy::VectraFlow::insert(uint64_t id) -> bool { 
    datas_.push_back(id);
    return true; 
}

/// VectraFlow 目前不支持删除
auto candy::VectraFlow::erase(uint64_t id) -> bool { return true; }


// 并行没搞明白 先不鸟它了
auto candy::VectraFlow::query(std::unique_ptr<VectorRecord>& record, int k) -> std::vector<uint64_t> {



    
    const auto rec = record.get();
    
    std :: priority_queue<std::pair<double, uint64_t>> pq;

    std::vector<double> selfquare(datas_.size());
    for (size_t i = 0; i < datas_.size(); i++) {
        auto rec = storage_manager_->getVectorByUid(datas_[i]).get();
        auto square = storage_manager_->engine_->getVectorSquareLength(rec->data_);
        selfquare.emplace_back(square);
    }

    auto input_vector_square = storage_manager_->engine_->getVectorSquareLength(rec->data_);

    for (size_t i = 0; i < datas_.size(); ++i) {
        
        //auto dist = storage_manager_->engine_->EuclideanDistance(rec->data_, local_rec->data_);

        // VectraFlow 特有的计算方式
        
        auto dist = input_vector_square + selfquare[i] - 
            2 * storage_manager_->engine_->dotmultiply(rec->data_, storage_manager_->getVectorByUid(datas_[i])->data_);

        //auto dist = storage_manager_->engine_->EuclideanDistance(rec->data_, storage_manager_->getVectorByUid(datas_[i])->data_);

        if (pq.size() < static_cast<size_t>(k)) {
            pq.emplace(dist, datas_[i]);
        } else if (dist < pq.top().first) {
            pq.emplace(dist, datas_[i]);
            pq.pop();
        }
    }
    std::vector<uint64_t> result;
    while (!pq.empty()) {
        result.push_back(pq.top().second);
        pq.pop();
    }
    reverse(result.begin(), result.end());
    return result;

    // // 并行计算距离
    // #ifdef _OPENMP
    // #pragma omp parallel for
    // #endif
    // for (size_t i = 0; i < datas_.size(); ++i) {
    //     const auto local_rec = storage_manager_->getVectorByUid(datas_[i]).get();
    //     //auto dist = storage_manager_->engine_->EuclideanDistance(rec->data_, local_rec->data_);

    //     // VectraFlow 特有的计算方式
    //     auto dist = input_vector_square + selfquare[i] - 
    //         2 * storage_manager_->engine_->dotmultiply(rec->data_, local_rec->data_);

    //     int thread_id = omp_get_thread_num(); // 获取当前线程的 ID
    //     // 对每个线程使用本地的优先队列
    //     if (pq_list[thread_id].size() < static_cast<size_t>(k)) {
    //         pq_list[thread_id].emplace(dist, datas_[i]);
    //     } else if (dist < pq_list[thread_id].top().first) {
    //         pq_list[thread_id].pop();
    //         pq_list[thread_id].emplace(dist, datas_[i]);
    //     }
    // }

    // // 合并各线程的结果
    // std::priority_queue<std::pair<double, uint64_t>> global_pq;
    // for (int t = 0; t < omp_get_max_threads(); ++t) {
    //     while (!pq_list[t].empty()) {
    //         auto item = pq_list[t].top();
    //         pq_list[t].pop();
    //         if (global_pq.size() < static_cast<size_t>(k)) {
    //             global_pq.emplace(item);
    //         } else if (item.first < global_pq.top().first) {
    //             global_pq.pop();
    //             global_pq.emplace(item);
    //         }
    //     }
    // }

    // // 提取最终的结果
    // std::vector<uint64_t> result;
    // while (!global_pq.empty()) {
    //     result.push_back(global_pq.top().second);
    //     global_pq.pop();
    // }
    // reverse(result.begin(), result.end());
    // return result;
}
