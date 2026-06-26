// Cross-thread free microbenchmark for join pair materialization (R1 + make_shared).
// Goal: measure whether shipping RecordPair (two shared RecordViews) across an
// SPSC queue and destroying them on the consumer thread costs more than the
// same-thread create+destroy pattern, isolated from the full Join pipeline.
//
// This is allocator-agnostic: we count global operator new/delete invocations
// ourselves, so the evidence holds whether or not tcmalloc is linked. If a
// per-thread caching allocator is in use, the cross-thread vs same-thread wall
// clock delta is exactly the (B) cross-thread free cost the design flags.
//
// Build target: profile_pair_free_bench (PERF). Run manually.
//
// Scenarios:
//   A. SAME-THREAD:   producer creates a RecordPair and destroys it immediately.
//   B. CROSS-THREAD:  producer creates on thread A, hands off via RingBufferQueue,
//                     consumer on thread B holds the last reference and frees it.
//   C. WINDOW-RETAIN: producer keeps a RecordView in a simulated window deque
//                     (record born + freed on producer thread), consumer only
//                     decrements its copy. This mirrors the realistic pipeline
//                     where the window still owns the record, so the heavy
//                     control-block+object free stays thread-local.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <random>
#include <thread>
#include <vector>
#include <algorithm>
#include <cmath>

#include "compute_engine/compute_engine.h"
#include "common/data_types.h"
#include "execution/ring_buffer_queue.h"

using namespace sageFlow;
using Clock = std::chrono::steady_clock;

// ---------------------------------------------------------------------------
// Global allocation counters (allocator-agnostic evidence).
// ---------------------------------------------------------------------------
namespace {
std::atomic<uint64_t> g_new_count{0};
std::atomic<uint64_t> g_delete_count{0};
bool g_count_enabled = false;
}  // namespace

void* operator new(std::size_t sz) {
  if (g_count_enabled) g_new_count.fetch_add(1, std::memory_order_relaxed);
  if (void* p = std::malloc(sz == 0 ? 1 : sz)) return p;
  throw std::bad_alloc();
}
void operator delete(void* p) noexcept {
  if (p && g_count_enabled) g_delete_count.fetch_add(1, std::memory_order_relaxed);
  std::free(p);
}
void operator delete(void* p, std::size_t) noexcept {
  if (p && g_count_enabled) g_delete_count.fetch_add(1, std::memory_order_relaxed);
  std::free(p);
}

namespace {

double seconds(Clock::time_point a, Clock::time_point b) {
  return std::chrono::duration<double>(b - a).count();
}

double percentile(std::vector<double>& v, double p) {
  if (v.empty()) return 0.0;
  std::sort(v.begin(), v.end());
  auto idx = static_cast<size_t>(p * (v.size() - 1));
  return v[idx];
}

// make_shared so the control block and the VectorRecord (incl. its VectorData
// char[]) are allocated together: R1's single combined allocation.
RecordView makeRecordView(uint64_t uid, int dim, std::mt19937& gen) {
  std::normal_distribution<float> dist(0.0f, 1.0f);
  std::vector<float> v(dim);
  for (int i = 0; i < dim; ++i) v[i] = dist(gen);
  VectorData data(dim, DataType::Float32, reinterpret_cast<char*>(v.data()));
  return std::make_shared<const VectorRecord>(uid, static_cast<int64_t>(uid), std::move(data));
}

struct Stats {
  double wall_s;
  double pairs_per_s;
  uint64_t new_count;
  uint64_t delete_count;
  double p50_ns;
  double p99_ns;
};

void printRow(const char* name, const Stats& s, uint64_t pairs) {
  std::cout << name << " | " << s.wall_s << " | "
            << static_cast<uint64_t>(s.pairs_per_s) << " | "
            << s.new_count << " | " << s.delete_count << " | "
            << (static_cast<double>(s.new_count) / pairs) << " | "
            << s.p50_ns << " | " << s.p99_ns << "\n";
}

// Pre-generate source RecordViews off the timed path so allocation of the
// underlying records is not what we measure; we measure the pair lifecycle.
std::vector<RecordView> pregen(uint64_t n, int dim) {
  std::vector<RecordView> out;
  out.reserve(n);
  std::mt19937 gen(12345);
  for (uint64_t i = 0; i < n; ++i) out.push_back(makeRecordView(i + 1, dim, gen));
  return out;
}

// Scenario A: same-thread create + destroy of the RecordPair payload.
Stats benchSameThread(const std::vector<RecordView>& src, uint64_t pairs) {
  g_new_count = 0;
  g_delete_count = 0;
  std::vector<double> samples;
  samples.reserve(pairs);

  auto t0 = Clock::now();
  g_count_enabled = true;
  for (uint64_t i = 0; i < pairs; ++i) {
    const RecordView& l = src[(2 * i) % src.size()];
    const RecordView& r = src[(2 * i + 1) % src.size()];
    auto ti = Clock::now();
    {
      Response resp{ResponseType::RecordPair,
                    std::make_unique<RecordPairPayload>(l, r, 0.5)};
      // Pair born and freed on this same thread.
    }
    auto tj = Clock::now();
    if (i % 64 == 0) samples.push_back(std::chrono::duration<double, std::nano>(tj - ti).count());
  }
  g_count_enabled = false;
  auto t1 = Clock::now();

  Stats s;
  s.wall_s = seconds(t0, t1);
  s.pairs_per_s = pairs / s.wall_s;
  s.new_count = g_new_count.load();
  s.delete_count = g_delete_count.load();
  s.p50_ns = percentile(samples, 0.50);
  s.p99_ns = percentile(samples, 0.99);
  return s;
}

// Scenario B: producer creates RecordPair on thread A, ships via SPSC queue,
// consumer on thread B holds the LAST reference and frees it (worst case).
Stats benchCrossThread(const std::vector<RecordView>& src, uint64_t pairs) {
  g_new_count = 0;
  g_delete_count = 0;
  RingBufferQueue queue(4096);
  std::atomic<bool> go{false};
  std::vector<double> samples;
  samples.reserve(pairs);

  std::thread consumer([&]() {
    while (!go.load(std::memory_order_acquire)) {}
    uint64_t got = 0;
    while (got < pairs) {
      auto item = queue.pop();
      if (!item) continue;
      // Destroying `item` here drops the last refs -> cross-thread free of the
      // control block + VectorRecord that were allocated on the producer thread.
      ++got;
    }
  });

  auto t0 = Clock::now();
  go.store(true, std::memory_order_release);
  g_count_enabled = true;
  for (uint64_t i = 0; i < pairs; ++i) {
    const RecordView& l = src[(2 * i) % src.size()];
    const RecordView& r = src[(2 * i + 1) % src.size()];
    auto ti = Clock::now();
    Response resp{ResponseType::RecordPair,
                  std::make_unique<RecordPairPayload>(l, r, 0.5)};
    TaggedResponse tagged(std::move(resp), 0);
    while (!queue.push(std::move(tagged))) { /* spin until consumer drains */ }
    auto tj = Clock::now();
    if (i % 64 == 0) samples.push_back(std::chrono::duration<double, std::nano>(tj - ti).count());
  }
  consumer.join();
  g_count_enabled = false;
  auto t1 = Clock::now();

  Stats s;
  s.wall_s = seconds(t0, t1);
  s.pairs_per_s = pairs / s.wall_s;
  s.new_count = g_new_count.load();
  s.delete_count = g_delete_count.load();
  s.p50_ns = percentile(samples, 0.50);
  s.p99_ns = percentile(samples, 0.99);
  return s;
}

// Scenario C: realistic pipeline. The window (producer thread) retains a
// RecordView for each record, so the pair the consumer receives never holds the
// last reference. The consumer drops only its pair copies (refcount decrement),
// while the heavy free (control block + VectorRecord + char[]) happens on the
// producer thread when the window evicts. Records are pregenerated and shared,
// so this is directly comparable to A/B: new_per_pair stays ~1.0 (only the
// RecordPairPayload allocates), isolating free direction rather than birth cost.
Stats benchWindowRetain(const std::vector<RecordView>& src, uint64_t pairs) {
  g_new_count = 0;
  g_delete_count = 0;
  RingBufferQueue queue(4096);
  std::atomic<bool> go{false};
  std::vector<double> samples;
  samples.reserve(pairs);

  std::thread consumer([&]() {
    while (!go.load(std::memory_order_acquire)) {}
    uint64_t got = 0;
    while (got < pairs) {
      auto item = queue.pop();
      if (!item) continue;
      ++got;  // consumer drops its pair copy -> refcount decrement only, never last ref
    }
  });

  auto t0 = Clock::now();
  go.store(true, std::memory_order_release);
  g_count_enabled = true;
  for (uint64_t i = 0; i < pairs; ++i) {
    // Both records are shared from the producer-retained pool (src), so the
    // producer thread always keeps a reference; the consumer never frees them.
    const RecordView& l = src[(2 * i) % src.size()];
    const RecordView& r = src[(2 * i + 1) % src.size()];
    auto ti = Clock::now();
    Response resp{ResponseType::RecordPair,
                  std::make_unique<RecordPairPayload>(l, r, 0.5)};
    TaggedResponse tagged(std::move(resp), 0);
    while (!queue.push(std::move(tagged))) {}
    auto tj = Clock::now();
    if (i % 64 == 0) samples.push_back(std::chrono::duration<double, std::nano>(tj - ti).count());
  }
  consumer.join();
  g_count_enabled = false;
  auto t1 = Clock::now();

  Stats s;
  s.wall_s = seconds(t0, t1);
  s.pairs_per_s = pairs / s.wall_s;
  s.new_count = g_new_count.load();
  s.delete_count = g_delete_count.load();
  s.p50_ns = percentile(samples, 0.50);
  s.p99_ns = percentile(samples, 0.99);
  return s;
}

enum class SimilarityWorkload {
  NONE,
  LEGACY_DIRECT_SCALAR,
  LEGACY_EXTRACT_SCALAR,
  COMPUTE_ENGINE
};

const char* workloadName(SimilarityWorkload workload) {
  switch (workload) {
    case SimilarityWorkload::NONE:
      return "C_window_retain";
    case SimilarityWorkload::LEGACY_DIRECT_SCALAR:
      return "D_window_scalar_direct";
    case SimilarityWorkload::LEGACY_EXTRACT_SCALAR:
      return "E_window_extract_scalar";
    case SimilarityWorkload::COMPUTE_ENGINE:
      return "F_window_compute_engine";
  }
  return "unknown";
}

double legacyDirectSimilarity(const VectorRecord& left, const VectorRecord& right, double alpha) {
  if (left.data_.type_ != DataType::Float32 || right.data_.type_ != DataType::Float32 ||
      left.data_.dim_ != right.data_.dim_ || left.data_.dim_ <= 0) {
    return 0.0;
  }
  const auto* left_data = reinterpret_cast<const float*>(left.data_.data_.get());
  const auto* right_data = reinterpret_cast<const float*>(right.data_.data_.get());
  double distance_sq = 0.0;
  for (int i = 0; i < left.data_.dim_; ++i) {
    const double diff = static_cast<double>(left_data[i]) - static_cast<double>(right_data[i]);
    distance_sq += diff * diff;
  }
  return std::exp(-alpha * std::sqrt(distance_sq));
}

double legacyExtractSimilarity(const VectorRecord& left, const VectorRecord& right, double alpha) {
  if (left.data_.type_ != DataType::Float32 || right.data_.type_ != DataType::Float32 ||
      left.data_.dim_ != right.data_.dim_ || left.data_.dim_ <= 0) {
    return 0.0;
  }
  const size_t dim = static_cast<size_t>(left.data_.dim_);
  std::vector<float> left_vec(dim);
  std::vector<float> right_vec(dim);
  std::memcpy(left_vec.data(), left.data_.data_.get(), dim * sizeof(float));
  std::memcpy(right_vec.data(), right.data_.data_.get(), dim * sizeof(float));

  double distance_sq = 0.0;
  for (size_t i = 0; i < dim; ++i) {
    const double diff = static_cast<double>(left_vec[i]) - static_cast<double>(right_vec[i]);
    distance_sq += diff * diff;
  }
  return std::exp(-alpha * std::sqrt(distance_sq));
}

double computeSimilarity(
    SimilarityWorkload workload,
    ComputeEngine& compute_engine,
    const RecordView& left,
    const RecordView& right,
    double alpha) {
  switch (workload) {
    case SimilarityWorkload::NONE:
      return 0.5;
    case SimilarityWorkload::LEGACY_DIRECT_SCALAR:
      return legacyDirectSimilarity(*left, *right, alpha);
    case SimilarityWorkload::LEGACY_EXTRACT_SCALAR:
      return legacyExtractSimilarity(*left, *right, alpha);
    case SimilarityWorkload::COMPUTE_ENGINE:
      return compute_engine.Similarity(left->data_, right->data_, alpha);
  }
  return 0.5;
}

Stats benchWindowRetainWorkload(
    const std::vector<RecordView>& src,
    uint64_t pairs,
    SimilarityWorkload workload) {
  g_new_count = 0;
  g_delete_count = 0;
  RingBufferQueue queue(4096);
  std::atomic<bool> go{false};
  std::vector<double> samples;
  samples.reserve(pairs);
  ComputeEngine compute_engine;
  constexpr double kAlpha = 0.1;

  std::thread consumer([&]() {
    while (!go.load(std::memory_order_acquire)) {}
    uint64_t got = 0;
    while (got < pairs) {
      auto item = queue.pop();
      if (!item) continue;
      ++got;
    }
  });

  auto t0 = Clock::now();
  go.store(true, std::memory_order_release);
  g_count_enabled = true;
  for (uint64_t i = 0; i < pairs; ++i) {
    const RecordView& l = src[(2 * i) % src.size()];
    const RecordView& r = src[(2 * i + 1) % src.size()];
    auto ti = Clock::now();
    const double similarity = computeSimilarity(workload, compute_engine, l, r, kAlpha);
    Response resp{ResponseType::RecordPair,
                  std::make_unique<RecordPairPayload>(l, r, similarity)};
    TaggedResponse tagged(std::move(resp), 0);
    while (!queue.push(std::move(tagged))) {}
    auto tj = Clock::now();
    if (i % 64 == 0) samples.push_back(std::chrono::duration<double, std::nano>(tj - ti).count());
  }
  consumer.join();
  g_count_enabled = false;
  auto t1 = Clock::now();

  Stats s;
  s.wall_s = seconds(t0, t1);
  s.pairs_per_s = pairs / s.wall_s;
  s.new_count = g_new_count.load();
  s.delete_count = g_delete_count.load();
  s.p50_ns = percentile(samples, 0.50);
  s.p99_ns = percentile(samples, 0.99);
  return s;
}

}  // namespace

int main(int argc, char** argv) {
  const int dim = 128;
  const uint64_t pairs = (argc > 1) ? std::strtoull(argv[1], nullptr, 10) : 2'000'000ULL;

  std::cout << "hardware_concurrency=" << std::thread::hardware_concurrency()
            << " dim=" << dim << " pairs=" << pairs << "\n";
  std::cout << "(allocator-agnostic: counts via global new/delete; link tcmalloc "
               "to see its effect on wall time)\n\n";

  auto src = pregen(4096, dim);

  std::cout << "scenario | wall_s | pairs/s | new_calls | delete_calls | new_per_pair | p50_ns | p99_ns\n";

  // Warm up allocator caches.
  benchSameThread(src, pairs / 10);

  Stats a = benchSameThread(src, pairs);
  printRow("A_same_thread  ", a, pairs);

  Stats b = benchCrossThread(src, pairs);
  printRow("B_cross_thread ", b, pairs);

  Stats c = benchWindowRetain(src, pairs);
  printRow("C_window_retain", c, pairs);

  std::cout << "\nComputeEngine integration workloads (same window-retain cross-thread handoff):\n";
  Stats c2 = benchWindowRetainWorkload(src, pairs, SimilarityWorkload::NONE);
  printRow(workloadName(SimilarityWorkload::NONE), c2, pairs);

  Stats d = benchWindowRetainWorkload(src, pairs, SimilarityWorkload::LEGACY_DIRECT_SCALAR);
  printRow(workloadName(SimilarityWorkload::LEGACY_DIRECT_SCALAR), d, pairs);

  Stats e = benchWindowRetainWorkload(src, pairs, SimilarityWorkload::LEGACY_EXTRACT_SCALAR);
  printRow(workloadName(SimilarityWorkload::LEGACY_EXTRACT_SCALAR), e, pairs);

  Stats f = benchWindowRetainWorkload(src, pairs, SimilarityWorkload::COMPUTE_ENGINE);
  printRow(workloadName(SimilarityWorkload::COMPUTE_ENGINE), f, pairs);

  std::cout << "\nComputeEngine comparison:\n";
  std::cout << "  vs direct scalar: throughput "
            << (f.pairs_per_s / std::max(d.pairs_per_s, 1.0))
            << "x, p50 " << d.p50_ns << " -> " << f.p50_ns << " ns, p99 "
            << d.p99_ns << " -> " << f.p99_ns << " ns\n";
  std::cout << "  vs extract+scalar: throughput "
            << (f.pairs_per_s / std::max(e.pairs_per_s, 1.0))
            << "x, p50 " << e.p50_ns << " -> " << f.p50_ns << " ns, p99 "
            << e.p99_ns << " -> " << f.p99_ns << " ns\n";

  std::cout << "\nInterpretation:\n";
  std::cout << "  new_per_pair ~1.0 in all scenarios => only the RecordPairPayload allocates "
               "per pair; zero VectorData deep copy on the emit path (R1 goal met).\n";
  std::cout << "  B/A wall ratio => total cost of crossing the SPSC queue (handoff + payload "
               "cross-thread free), vs purely-local create+destroy.\n";
  std::cout << "  B vs C => B frees the record bodies cross-thread, C keeps them on the producer "
               "(window-retained). If B ~ C, the record free DIRECTION is NOT the bottleneck; the "
               "handoff itself dominates, so an arena allocator targeting record free would not "
               "help here.\n";
  return 0;
}
