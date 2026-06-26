// Concurrency data-plane microbenchmark (profiling evidence, not a gtest).
// Goal: directly measure whether the StorageManager global write lock and the
// per-Knn lock serialize concurrent access, isolated from the full Join pipeline.
//
// Build target: profile_concurrency_bench (PERF). Run manually.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <random>
#include <thread>
#include <vector>

#include "compute_engine/compute_engine.h"
#include "index/knn.h"
#include "storage/storage_manager.h"

using namespace sageFlow;
using Clock = std::chrono::steady_clock;

namespace {

std::unique_ptr<VectorRecord> makeRecord(uint64_t uid, int dim, std::mt19937& gen) {
  std::normal_distribution<float> dist(0.0f, 1.0f);
  std::vector<float> v(dim);
  for (int i = 0; i < dim; ++i) v[i] = dist(gen);
  auto raw = std::make_unique<char[]>(dim * sizeof(float));
  std::memcpy(raw.get(), v.data(), dim * sizeof(float));
  VectorData data(dim, DataType::Float32, raw.release());
  return std::make_unique<VectorRecord>(uid, static_cast<int64_t>(uid), std::move(data));
}

double seconds(Clock::time_point a, Clock::time_point b) {
  return std::chrono::duration<double>(b - a).count();
}

// Benchmark 1: concurrent StorageManager::insert across T threads.
// Each thread inserts its own disjoint uid range -> no logical conflict,
// so any slowdown vs T=1 is pure lock contention on the single map_mutex_.
void benchStorageInsert(int dim, int per_thread, const std::vector<int>& thread_counts) {
  std::cout << "\n== Bench1: StorageManager concurrent insert (disjoint uids) ==\n";
  std::cout << "dim=" << dim << " inserts_per_thread=" << per_thread << "\n";
  std::cout << "threads | total_inserts | wall_s | inserts/s | scaling_vs_1\n";

  double base_throughput = 0.0;
  for (int T : thread_counts) {
    auto storage = std::make_shared<StorageManager>();
    storage->engine_ = std::make_shared<ComputeEngine>();

    // Pre-generate records off the timed path.
    std::vector<std::vector<std::unique_ptr<VectorRecord>>> per_thread_recs(T);
    for (int t = 0; t < T; ++t) {
      std::mt19937 gen(1000 + t);
      per_thread_recs[t].reserve(per_thread);
      for (int i = 0; i < per_thread; ++i) {
        uint64_t uid = static_cast<uint64_t>(t) * per_thread + i + 1;
        per_thread_recs[t].push_back(makeRecord(uid, dim, gen));
      }
    }

    std::atomic<bool> go{false};
    std::vector<std::thread> threads;
    auto t0 = Clock::now();
    for (int t = 0; t < T; ++t) {
      threads.emplace_back([&, t]() {
        while (!go.load(std::memory_order_acquire)) {}
        for (auto& r : per_thread_recs[t]) {
          storage->insert(std::move(r));
        }
      });
    }
    t0 = Clock::now();
    go.store(true, std::memory_order_release);
    for (auto& th : threads) th.join();
    auto t1 = Clock::now();

    double wall = seconds(t0, t1);
    uint64_t total = static_cast<uint64_t>(T) * per_thread;
    double thr = total / wall;
    if (T == 1) base_throughput = thr;
    std::cout << T << " | " << total << " | " << wall << " | "
              << static_cast<uint64_t>(thr) << " | "
              << (base_throughput > 0 ? thr / base_throughput : 1.0) << "x\n";
  }
}

// Benchmark 2: per-Knn local index single-writer + single-reader, mirrors VSJoin
// local index access. T independent (Knn, shared StorageManager) pairs run in
// parallel; each pair has 1 writer thread (insert) + 1 reader thread (query_for_join).
// This shows whether the per-index lock limits aggregate throughput as pairs scale.
void benchKnnLocal(int dim, int per_index, const std::vector<int>& pair_counts) {
  std::cout << "\n== Bench2: per-Knn local index, 1 writer + 1 reader per index ==\n";
  std::cout << "dim=" << dim << " ops_per_index=" << per_index << "\n";
  std::cout << "pairs | wall_s | writer_ops/s | reader_queries/s | scaling_vs_1\n";

  double base = 0.0;
  for (int P : pair_counts) {
    auto storage = std::make_shared<StorageManager>();
    storage->engine_ = std::make_shared<ComputeEngine>();

    std::vector<std::unique_ptr<Knn>> indexes;
    std::vector<std::vector<std::unique_ptr<VectorRecord>>> recs(P);
    std::vector<std::unique_ptr<VectorRecord>> queries;
    for (int p = 0; p < P; ++p) {
      auto knn = std::make_unique<Knn>();
      knn->storage_manager_ = storage;
      indexes.push_back(std::move(knn));
      std::mt19937 gen(7000 + p);
      recs[p].reserve(per_index);
      for (int i = 0; i < per_index; ++i) {
        uint64_t uid = static_cast<uint64_t>(p) * per_index + i + 1;
        // Insert into shared storage up-front so reader can resolve.
        storage->insert(makeRecord(uid, dim, gen));
        recs[p].push_back(makeRecord(uid, dim, gen));  // uid reused only as id holder
      }
      queries.push_back(makeRecord(900000000ULL + p, dim, gen));
    }

    std::atomic<bool> go{false};
    std::atomic<uint64_t> reader_ops{0};
    std::vector<std::thread> threads;
    for (int p = 0; p < P; ++p) {
      // writer
      threads.emplace_back([&, p]() {
        while (!go.load(std::memory_order_acquire)) {}
        uint64_t base_uid = static_cast<uint64_t>(p) * per_index + 1;
        for (int i = 0; i < per_index; ++i) {
          indexes[p]->insert(base_uid + i);
        }
      });
      // reader
      threads.emplace_back([&, p]() {
        while (!go.load(std::memory_order_acquire)) {}
        uint64_t local = 0;
        for (int i = 0; i < per_index; ++i) {
          auto ids = indexes[p]->query_for_join(*queries[p], 0.0, 0.1);
          local += ids.size();
        }
        reader_ops.fetch_add(local == 0 ? 1 : per_index, std::memory_order_relaxed);
      });
    }
    auto t0 = Clock::now();
    go.store(true, std::memory_order_release);
    for (auto& th : threads) th.join();
    auto t1 = Clock::now();

    double wall = seconds(t0, t1);
    uint64_t writer_total = static_cast<uint64_t>(P) * per_index;
    uint64_t reader_total = static_cast<uint64_t>(P) * per_index;
    double w_thr = writer_total / wall;
    if (P == 1) base = w_thr;
    std::cout << P << " | " << wall << " | " << static_cast<uint64_t>(w_thr)
              << " | " << static_cast<uint64_t>(reader_total / wall) << " | "
              << (base > 0 ? w_thr / base : 1.0) << "x\n";
  }
}

}  // namespace

int main() {
  const int dim = 128;
  const std::vector<int> threads = {1, 2, 4, 8};

  std::cout << "hardware_concurrency=" << std::thread::hardware_concurrency() << "\n";
  benchStorageInsert(dim, 50000, threads);
  benchKnnLocal(dim, 20000, threads);
  return 0;
}
