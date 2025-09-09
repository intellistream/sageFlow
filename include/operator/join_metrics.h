#pragma once
#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <filesystem>
#include <fstream>
#include <mutex>

namespace candy {
struct JoinMetrics {
  std::atomic<uint64_t> window_insert_ns{0};
  std::atomic<uint64_t> index_insert_ns{0};
  std::atomic<uint64_t> expire_ns{0};
  std::atomic<uint64_t> candidate_fetch_ns{0};
  std::atomic<uint64_t> similarity_ns{0};
  std::atomic<uint64_t> join_function_ns{0};
  std::atomic<uint64_t> emit_ns{0};
  std::atomic<uint64_t> lock_wait_ns{0};
  std::atomic<uint64_t> total_records_left{0};
  std::atomic<uint64_t> total_records_right{0};
  std::atomic<uint64_t> total_emits{0};

  static JoinMetrics& instance() {
    static JoinMetrics inst; return inst;
  }
  void reset() {
    window_insert_ns = index_insert_ns = expire_ns = candidate_fetch_ns = similarity_ns = join_function_ns = emit_ns = lock_wait_ns = 0;
    total_records_left = total_records_right = total_emits = 0;
  }
  void dump_tsv(const std::string& path) {
    std::error_code ec; std::filesystem::create_directories(std::filesystem::path(path).parent_path(), ec);
    std::ofstream ofs(path, std::ios::out | std::ios::trunc);
    if(!ofs) return;
    ofs << "metric\tvalue\n";
#define EMIT(m) ofs<<#m"\t"<<m.load()<<"\n";
    EMIT(window_insert_ns) EMIT(index_insert_ns) EMIT(expire_ns) EMIT(candidate_fetch_ns) EMIT(similarity_ns)
    EMIT(join_function_ns) EMIT(emit_ns) EMIT(lock_wait_ns) EMIT(total_records_left) EMIT(total_records_right) EMIT(total_emits)
#undef EMIT
  }
};

class ScopedTimerAtomic {
 public:
  using Clock = std::chrono::high_resolution_clock;
  explicit ScopedTimerAtomic(std::atomic<uint64_t>& slot) : slot_(slot), start_(Clock::now()) {}
  ~ScopedTimerAtomic() {
    auto d = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now()-start_).count();
    slot_.fetch_add(static_cast<uint64_t>(d), std::memory_order_relaxed);
  }
 private:
  std::atomic<uint64_t>& slot_;
  Clock::time_point start_;
};

class ScopedAccumulateAtomic {
 public:
  ScopedAccumulateAtomic(std::atomic<uint64_t>& slot, uint64_t start_ns) : slot_(slot), start_ns_(start_ns) {}
  ~ScopedAccumulateAtomic() {
    uint64_t end_ns = now_ns(); slot_.fetch_add(end_ns - start_ns_, std::memory_order_relaxed);
  }
  static uint64_t now_ns() {
    return (uint64_t) std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();
  }
 private:
  std::atomic<uint64_t>& slot_;
  uint64_t start_ns_;
};

} // namespace candy
