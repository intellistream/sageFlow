#include "index/vectraflow.h"
#include <queue>
#include <omp.h>

candy::VectraFlow::~VectraFlow() = default;

auto candy::VectraFlow::insert(uint64_t id) -> bool { 
    datas.push_back(id);
    return true; 
}

// TODO: 那就不支持 erase 罢（
auto candy::VectraFlow::erase(uint64_t id) -> bool { return true; }

auto candy::VectraFlow::query(std::unique_ptr<VectorRecord>& record, int k) -> std::vector<int32_t> {
    const auto rec = record.get();
    std::vector<std::priority_queue<std::pair<double, int32_t>>> pq_list(omp_get_max_threads());

		std::vector<double> selfquare(datas.size());
		for (int i = 0; i < datas.size(); i ++) {
			auto rec = storage_manager_->getVectorByUid(datas[i]).get();
			auto square = storage_manager_->engine_->getVectorSquareLength(rec->data_);
			selfquare.emplace_back(square);
		}

		auto input_vector_square = storage_manager_->engine_->getVectorSquareLength(rec->data_);

    // 并行计算距离
    #pragma omp parallel for
    for (int i = 0; i < datas.size(); ++i) {
        const auto local_rec = storage_manager_->getVectorByUid(datas[i]).get();
        //auto dist = storage_manager_->engine_->EuclideanDistance(rec->data_, local_rec->data_);

				// VectraFlow 特有的计算方式
				auto dist = input_vector_square + selfquare[i] - 
					2 * storage_manager_->engine_->dotmultiply(rec->data_, local_rec->data_);

        int thread_id = omp_get_thread_num(); // 获取当前线程的 ID
        // 对每个线程使用本地的优先队列
        if (pq_list[thread_id].size() < k) {
            pq_list[thread_id].emplace(dist, datas[i]);
        } else if (dist < pq_list[thread_id].top().first) {
            pq_list[thread_id].pop();
            pq_list[thread_id].emplace(dist, datas[i]);
        }
    }

    // 合并各线程的结果
    std::priority_queue<std::pair<double, int32_t>> global_pq;
    for (int t = 0; t < omp_get_max_threads(); ++t) {
        while (!pq_list[t].empty()) {
            auto item = pq_list[t].top();
            pq_list[t].pop();
            if (global_pq.size() < k) {
                global_pq.emplace(item);
            } else if (item.first < global_pq.top().first) {
                global_pq.pop();
                global_pq.emplace(item);
            }
        }
    }

    // 提取最终的结果
    std::vector<int32_t> result;
    while (!global_pq.empty()) {
        int32_t id_in_storage;
        auto rec = storage_manager_->getVectorByUid(global_pq.top().second, id_in_storage).get();
        result.push_back(id_in_storage);
        global_pq.pop();
    }
    reverse(result.begin(), result.end());
    return result;
}
