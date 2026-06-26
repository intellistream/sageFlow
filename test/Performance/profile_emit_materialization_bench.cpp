// Emit-path materialization microbenchmark: CONCAT vs PAIR_PASSTHROUGH.
// Goal: quantify the performance gain of pair materialization on the join emit
// path, by running the SAME matched pairs through both modes of the real
// JoinResultEmitter and measuring wall time + per-pair latency.
//
// Allocation behavior (analytic, from the code, per matched pair):
//   CONCAT (legacy): appendJoinedResult deep-copies probe and candidate into
//     unique_ptr<VectorRecord> (to satisfy the JoinFunc unique_ptr& signature)
//     -> 2 records + 2 char[] of size dim*4 bytes; then the join function builds
//     one concatenated 2*dim record -> +1 record +1 char[] of 2*dim*4 bytes.
//     => heap traffic grows linearly with dimension.
//   PAIR_PASSTHROUGH: appendPair allocates one fixed-size RecordPairPayload
//     (two shared_ptr refcount bumps, no VectorData copy).
//     => heap traffic is constant, independent of dimension.
//
// We do NOT count global new/delete here: the emitter runs inside libsageflow,
// and a global operator new override in this executable does not reliably
// interpose into the already-linked shared library, so such counts would be
// misleading. Wall time and per-pair latency ARE reliable (they measure the
// real copy+alloc work regardless of which allocator services it), and are the
// authoritative signal for the performance benefit.
//
// Build target: profile_emit_materialization_bench (PERF). Run manually.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <random>
#include <vector>

#include "common/data_types.h"
#include "function/join_function.h"
#include "operator/join_operator_components/join_result_emitter.h"
#include "operator/utils/join_strategy_config.h"

using namespace sageFlow;
using Clock = std::chrono::steady_clock;

namespace {

double seconds(Clock::time_point a, Clock::time_point b) {
  return std::chrono::duration<double>(b - a).count();
}

double percentile(std::vector<double>& v, double p) {
  if (v.empty()) return 0.0;
  std::sort(v.begin(), v.end());
  return v[static_cast<size_t>(p * (v.size() - 1))];
}

RecordView makeRecordView(uint64_t uid, int dim, std::mt19937& gen) {
  std::normal_distribution<float> dist(0.0f, 1.0f);
  std::vector<float> v(dim);
  for (int i = 0; i < dim; ++i) v[i] = dist(gen);
  VectorData data(dim, DataType::Float32, reinterpret_cast<char*>(v.data()));
  return std::make_shared<const VectorRecord>(uid, static_cast<int64_t>(uid), std::move(data));
}

std::vector<RecordView> makeWarmSource(int dim, std::mt19937& gen) {
  std::vector<RecordView> src;
  src.reserve(4096);
  for (uint64_t i = 0; i < 4096; ++i) src.push_back(makeRecordView(i + 1, dim, gen));
  return src;
}

// Concat join function: mirrors the datasource test (output = 2*dim vector).
std::unique_ptr<JoinFunction> makeConcatJoinFunction(int dim) {
  return std::make_unique<JoinFunction>(
      "ConcatJoin",
      [](std::unique_ptr<VectorRecord>& left,
         std::unique_ptr<VectorRecord>& right) -> std::unique_ptr<VectorRecord> {
        const auto& lvd = left->data_;
        const auto& rvd = right->data_;
        int ld = lvd.dim_, rd = rvd.dim_;
        auto raw = std::make_unique<char[]>(static_cast<size_t>(ld + rd) * sizeof(float));
        std::memcpy(raw.get(), lvd.data_.get(), static_cast<size_t>(ld) * sizeof(float));
        std::memcpy(raw.get() + static_cast<size_t>(ld) * sizeof(float), rvd.data_.get(),
                    static_cast<size_t>(rd) * sizeof(float));
        VectorData vd(ld + rd, DataType::Float32, raw.release());
        const uint64_t id = left->uid_ * 1000000 + right->uid_ % 1000000;
        return std::make_unique<VectorRecord>(id, std::max(left->timestamp_, right->timestamp_), std::move(vd));
      },
      dim);
}

struct Stats {
  double wall_s;
  double pairs_per_s;
  double p50_ns;
  double p99_ns;
};

void printRow(const char* mode, int dim, const Stats& s) {
  std::cout << mode << " | " << dim << " | " << s.wall_s << " | "
            << static_cast<uint64_t>(s.pairs_per_s) << " | "
            << s.p50_ns << " | " << s.p99_ns << "\n";
}

Stats runConcat(const std::vector<RecordView>& src, uint64_t pairs, int dim, int left_slot) {
  auto join_fn = makeConcatJoinFunction(dim);
  JoinResultEmitter emitter(join_fn.get(), left_slot, MaterializationMode::CONCAT);
  std::vector<double> samples;
  samples.reserve(pairs / 64 + 1);

  std::vector<JoinOutputItem> out;
  out.reserve(1);
  auto t0 = Clock::now();
  for (uint64_t i = 0; i < pairs; ++i) {
    const VectorRecord& probe = *src[(2 * i) % src.size()];
    const VectorRecord& cand = *src[(2 * i + 1) % src.size()];
    auto ti = Clock::now();
    emitter.appendJoinedResult(probe, cand, left_slot, out);
    out.clear();  // consumer would take ownership; here we just drop it
    auto tj = Clock::now();
    if (i % 64 == 0) samples.push_back(std::chrono::duration<double, std::nano>(tj - ti).count());
  }
  auto t1 = Clock::now();

  Stats s;
  s.wall_s = seconds(t0, t1);
  s.pairs_per_s = pairs / s.wall_s;
  s.p50_ns = percentile(samples, 0.50);
  s.p99_ns = percentile(samples, 0.99);
  return s;
}

Stats runPair(const std::vector<RecordView>& src, uint64_t pairs, int dim, int left_slot) {
  JoinResultEmitter emitter(nullptr, left_slot, MaterializationMode::PAIR_PASSTHROUGH);
  std::vector<double> samples;
  samples.reserve(pairs / 64 + 1);

  std::vector<JoinOutputItem> out;
  out.reserve(1);
  auto t0 = Clock::now();
  for (uint64_t i = 0; i < pairs; ++i) {
    const RecordView& probe = src[(2 * i) % src.size()];
    const RecordView& cand = src[(2 * i + 1) % src.size()];
    auto ti = Clock::now();
    emitter.appendPair(probe, cand, left_slot, 0.5, out);
    out.clear();
    auto tj = Clock::now();
    if (i % 64 == 0) samples.push_back(std::chrono::duration<double, std::nano>(tj - ti).count());
  }
  auto t1 = Clock::now();

  Stats s;
  s.wall_s = seconds(t0, t1);
  s.pairs_per_s = pairs / s.wall_s;
  s.p50_ns = percentile(samples, 0.50);
  s.p99_ns = percentile(samples, 0.99);
  return s;
}

}  // namespace

int main(int argc, char** argv) {
  const uint64_t pairs = (argc > 1) ? std::strtoull(argv[1], nullptr, 10) : 1'000'000ULL;
  const std::vector<int> dims = {128, 384, 768};
  const int left_slot = 0;

  std::cout << "emit-path materialization bench: CONCAT vs PAIR_PASSTHROUGH\n";
  std::cout << "pairs=" << pairs << " per mode/dim\n";
  std::cout << "Analytic heap traffic per pair: CONCAT = 3 allocs (2 record copies "
               "of dim*4B + 1 concat of 2*dim*4B); PAIR = 1 fixed-size payload.\n\n";
  std::cout << "mode | dim | wall_s | pairs/s | p50_ns | p99_ns\n";

  for (int dim : dims) {
    std::mt19937 gen(4242 + dim);
    auto src = makeWarmSource(dim, gen);

    runConcat(src, pairs / 20, dim, left_slot);  // warm up
    Stats c = runConcat(src, pairs, dim, left_slot);
    printRow("CONCAT", dim, c);

    runPair(src, pairs / 20, dim, left_slot);  // warm up
    Stats p = runPair(src, pairs, dim, left_slot);
    printRow("PAIR  ", dim, p);

    std::cout << "  -> dim=" << dim
              << ": throughput " << (p.pairs_per_s / std::max(c.pairs_per_s, 1.0)) << "x; "
              << "p50 " << c.p50_ns << " -> " << p.p50_ns << " ns ("
              << (c.p50_ns / std::max(p.p50_ns, 1e-9)) << "x lower); "
              << "p99 " << c.p99_ns << " -> " << p.p99_ns << " ns\n\n";
  }
  return 0;
}
