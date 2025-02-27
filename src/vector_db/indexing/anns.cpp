#include <vector_db/indexing/anns.h>

#include <algorithm>
#include <cmath>
#include <numeric>
#include <stdexcept>

namespace candy {

void ANNS::insert(const std::shared_ptr<VectorRecord> &record) { index[record->id_] = record; }

auto ANNS::search(const VectorData &query, size_t k) -> std::vector<std::shared_ptr<VectorRecord>> {
  if (k > index.size()) {
    throw std::invalid_argument("k is larger than the number of vectors in the index.");
  }

  std::vector<std::pair<double, std::shared_ptr<VectorRecord>>> scored_results;

  for (const auto &[id, record] : index) {
    double similarity = std::inner_product(query.begin(), query.end(), record->data_->begin(), 0.0);
    scored_results.emplace_back(similarity, record);
  }

  std::ranges::sort(scored_results, std::greater());

  std::vector<std::shared_ptr<VectorRecord>> top_k;
  for (size_t i = 0; i < k; ++i) {
    top_k.push_back(scored_results[i].second);
  }

  return top_k;
}

void ANNS::remove(const std::string &id) { index.erase(id); }

}  // namespace candy