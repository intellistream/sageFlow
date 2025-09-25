#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <memory>
#include <vector>
#include <filesystem>
#include <atomic>
#include "operator/join_operator.h"
#include "stream/stream_environment.h"
#include "stream/data_stream_source/data_stream_source.h"
#include "function/sink_function.h"
#include "function/join_function.h"
#include "test_utils/test_data_generator.h"
#include "operator/join_metrics.h"
#include "concurrency/concurrency_manager.h"
#include "storage/storage_manager.h"
#include "test_utils/test_data_adapter.h"
#include "execution/collector.h"
#include "utils/logger.h"
#include "test_utils/dynamic_config.h"
#include "utils/log_config.h"
#include <fstream>
#include <set>
#include <sstream>
#include <algorithm>
#include <cmath>

namespace candy {
namespace test {

// 本地复制 TestVectorStreamSource（避免跨测试文件定义冲突/依赖）
class TestVectorStreamSource : public DataStreamSource {
 public:
  explicit TestVectorStreamSource(std::string name, std::vector<std::unique_ptr<VectorRecord>> records)
      : DataStreamSource(std::move(name), DataStreamSourceType::None), records_(std::move(records)) {}
  void Init() override { idx_=0; }
  auto Next() -> std::unique_ptr<VectorRecord> override {
    if (idx_ >= records_.size()) return nullptr;
    return std::move(records_[idx_++]);
  }
 private:
  std::vector<std::unique_ptr<VectorRecord>> records_;
  size_t idx_{0};
};

class JoinPerformanceTest : public ::testing::Test {
protected:
  void SetUp() override {
    JoinMetrics::instance().reset();
    auto storage = std::make_shared<StorageManager>();
    concurrency_manager_ = std::make_shared<ConcurrencyManager>(storage);
  }

  void TearDown() override {
    // 输出性能指标到文件
  std::filesystem::create_directories("build/metrics");
  std::string metrics_path = "build/metrics/join_perf_" + 
        std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".tsv";
    JoinMetrics::instance().dump_tsv(metrics_path);
  CANDY_LOG_INFO("TEST", "Performance metrics saved path={} ", metrics_path);
  }

  // 创建JoinFunction（使用 test_data_adapter 助手），支持配置维度与窗口
  std::unique_ptr<JoinFunction> createSimpleJoinFunction(int vector_dim, uint64_t win_ms, uint64_t trig_ms) {
    auto join_func_lambda = [](std::unique_ptr<VectorRecord>& left, 
                              std::unique_ptr<VectorRecord>& right) -> std::unique_ptr<VectorRecord> {
      auto lv = extractFloatVector(*left);
      auto rv = extractFloatVector(*right);
      std::vector<float> out;
      out.reserve(lv.size() + rv.size());
      out.insert(out.end(), lv.begin(), lv.end());
      out.insert(out.end(), rv.begin(), rv.end());
      uint64_t id = left->uid_ * 1000000 + right->uid_;
      int64_t ts = std::max(left->timestamp_, right->timestamp_);
      return createVectorRecord(id, ts, out);
    };
    auto jf = std::make_unique<JoinFunction>("SimpleJoin", join_func_lambda, vector_dim);
    jf->setWindow(win_ms, trig_ms);
    return jf;
  }

protected:
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
};

struct PerfConfigSets {
  std::vector<std::string> methods{ "bruteforce_eager","bruteforce_lazy","ivf_eager","ivf_lazy" };
  std::vector<int> sizes{1000};
  std::vector<int> parallelism{1};
  double threshold{0.8};
  // 支持多窗口测试：window_time_ms 可为数组
  std::vector<uint64_t> win_ms_list{2000};
  uint64_t trig_ms{500};
  int vector_dim{128};
  int64_t time_interval_ms{100};
};

struct PerfCaseParam { std::string method; int size; int parallelism; uint64_t win_ms; };

inline PerfConfigSets loadPerfConfig() {
  PerfConfigSets out; 
  // 使用新的动态配置系统
  std::vector<DynamicConfig> perf_configs;
  if (DynamicConfigManager::loadConfigs("config/perf_join.toml", "performance_test", perf_configs) && !perf_configs.empty()) {
    // 目前只取第一个配置块，若需要可扩展成多组
    const auto& config = perf_configs.front();
    
  out.threshold = config.get<double>("similarity_threshold", out.threshold);
  // 注意：DynamicConfig 将整数优先存为 int，因此这里按 int 读取以避免类型不匹配导致默认值生效
  // window_time_ms 既支持数组也支持单值
  {
    auto win_list = config.get<std::vector<int>>("window_time_ms", std::vector<int>{});
    if (!win_list.empty()) {
      out.win_ms_list.clear();
      out.win_ms_list.reserve(win_list.size());
      for (int v : win_list) out.win_ms_list.push_back(static_cast<uint64_t>(v));
    } else {
      int win_single = config.get<int>("window_time_ms", 2000);
      out.win_ms_list = { static_cast<uint64_t>(win_single) };
    }
  }
    
  auto trigger_ms = config.get<int>("window_trigger_ms", 0);
  if (trigger_ms > 0) out.trig_ms = static_cast<uint64_t>(trigger_ms);
    
    out.methods = config.get<std::vector<std::string>>("methods", out.methods);
    out.parallelism = config.get<std::vector<int>>("parallelism", out.parallelism);
  out.vector_dim = config.get<int>("vector_dim", out.vector_dim);
    // 读取 time_interval（可选）
    out.time_interval_ms = config.get<int>("time_interval", static_cast<int>(out.time_interval_ms));
    
    // 优先使用sizes，否则使用records_count
    auto sizes = config.get<std::vector<int>>("sizes", std::vector<int>{});
    if (!sizes.empty()) {
      out.sizes = sizes;
    } else {
      auto records_count = config.get<int>("records_count", 1000);
      out.sizes = {records_count};
    }
    // 配置回显（一次性）
    std::stringstream ss;
    ss << "methods=["; for (size_t i=0;i<out.methods.size();++i){ if(i) ss<<","; ss<<out.methods[i]; } ss<<"] sizes=[";
    for (size_t i=0;i<out.sizes.size();++i){ if(i) ss<<","; ss<<out.sizes[i]; } ss<<"] parallelism=[";
    for (size_t i=0;i<out.parallelism.size();++i){ if(i) ss<<","; ss<<out.parallelism[i]; }
    ss<<"] threshold="<<out.threshold<<" win_ms=[";
    for (size_t i=0;i<out.win_ms_list.size();++i){ if(i) ss<<","; ss<<out.win_ms_list[i]; }
    ss<<"] trig_ms="<<out.trig_ms<<" vector_dim="<<out.vector_dim
      <<" time_interval_ms="<<out.time_interval_ms;
    CANDY_LOG_INFO("TEST", "[CONFIG] {}", ss.str());
  }
  // 同时检查全局配置中的日志级别设置
  DynamicConfig global_config;
  if (DynamicConfigManager::loadConfig("config/perf_join.toml", "", global_config)) {
    auto log_level = global_config.get<std::string>("log.level", "info");
    std::cout << "[PerfTest] Setting log level to: " << log_level << std::endl;
    candy::init_log_level(log_level);
  }
  
  return out;
}

// 基于当前两流策略（右流=左流复制+UID偏移，同步时间戳与向量），计算交叉流期望匹配对
// 规则：
// 1) 所有左记录与其右侧复制记录必然匹配（相同向量，相同时间）
// 2) 正样本对(两条邻接记录)在跨流方向也匹配两次（L1-R2 与 L2-R1），near_threshold_pairs 中偶数下标(i%2==0)为 >= 阈值
// 3) 负样本对与随机尾部仅计入自匹配（随机高相似度概率极低，忽略）
static inline double l2_distance(const std::vector<float>& a, const std::vector<float>& b) {
  double acc = 0.0;
  const size_t n = std::min(a.size(), b.size());
  for (size_t i = 0; i < n; ++i) {
    const double d = static_cast<double>(a[i]) - static_cast<double>(b[i]);
    acc += d * d;
  }
  return std::sqrt(acc);
}

// 穷举遍历左右流，按窗口与相似度阈值精准计算期望匹配集合
static std::unordered_set<std::pair<uint64_t,uint64_t>, PairHash>
  computeExpectedPairsByTraversal(
    const std::vector<std::unique_ptr<VectorRecord>>& left_records,
    const std::vector<std::unique_ptr<VectorRecord>>& right_records,
    double similarity_threshold,
    uint64_t window_ms,
    double alpha = 0.1,
    uint64_t modulo_base = 1000000ULL) {
  // 使用双指针按时间窗口滑动，避免 O(N^2) 穷举
  std::unordered_set<std::pair<uint64_t,uint64_t>, PairHash> expected;
  expected.reserve(left_records.size());

  const int64_t w = static_cast<int64_t>(window_ms);
  size_t j_low = 0;   // 右侧窗口下界（满足 t_r >= t_l - w）
  size_t j_high = 0;  // 右侧窗口上界开区间（满足 t_r <= t_l + w 的下一个位置）

  const size_t R = right_records.size();
  for (const auto& l : left_records) {
    if (!l) continue;
    const int64_t tl = l->timestamp_;

    // 移动下界：跳过过早的右记录（t_r < tl - w）
    while (j_low < R) {
      const auto& rr = right_records[j_low];
      if (!rr) { ++j_low; continue; }
      if (rr->timestamp_ >= tl - w) break;
      ++j_low;
    }

    // 确保上界不小于下界
    if (j_high < j_low) j_high = j_low;

    // 扩展上界：包含所有满足 t_r <= tl + w 的右记录
    while (j_high < R) {
      const auto& rr = right_records[j_high];
      if (!rr) { ++j_high; continue; }
      if (rr->timestamp_ > tl + w) break;
      ++j_high;
    }

    // 计算相似度并收集窗口内的匹配
    const auto lv = extractFloatVector(*l);
    for (size_t j = j_low; j < j_high; ++j) {
      const auto& r = right_records[j];
      if (!r) continue;
      const auto rv = extractFloatVector(*r);
      const double dist = l2_distance(lv, rv);
      const double sim = std::exp(-alpha * dist);
      if (sim >= similarity_threshold) {
        expected.insert({l->uid_, r->uid_ % modulo_base});
      }
    }
  }

  return expected;
}

class JoinScalingTest : public ::testing::TestWithParam<PerfCaseParam> {
protected:
  void SetUp() override {
    JoinMetrics::instance().reset();
    concurrency_manager_ = std::make_shared<ConcurrencyManager>(std::make_shared<StorageManager>());
  }

  std::unique_ptr<Function> createSimpleJoinFunction() {
    auto join_func_lambda = [](std::unique_ptr<VectorRecord>& left, 
                              std::unique_ptr<VectorRecord>& right) -> std::unique_ptr<VectorRecord> {
  auto lv = extractFloatVector(*left);
  auto rv = extractFloatVector(*right);
  std::vector<float> out;
  out.reserve(lv.size() + rv.size());
  out.insert(out.end(), lv.begin(), lv.end());
  out.insert(out.end(), rv.begin(), rv.end());
  uint64_t id = left->uid_ * 1000000 + right->uid_;
  int64_t ts = std::max(left->timestamp_, right->timestamp_);
  return createVectorRecord(id, ts, out);
    };
    
    return std::make_unique<JoinFunction>("SimpleJoin", join_func_lambda, 128);
  }

protected:
  std::shared_ptr<ConcurrencyManager> concurrency_manager_;
};

TEST_P(JoinScalingTest, PerformanceScaling) {
  auto p = GetParam();
  auto method = p.method; int data_size = p.size; int parallelism = p.parallelism; uint64_t win_ms = p.win_ms;

  // 断言：data_size 必须来自 perf_join.toml 的 sizes 列表
  {
    static PerfConfigSets g_sets = loadPerfConfig();
    bool in_list = std::find(g_sets.sizes.begin(), g_sets.sizes.end(), data_size) != g_sets.sizes.end();
    EXPECT_TRUE(in_list) << "data_size=" << data_size << " not found in perf_join.toml sizes list";
  }

  // 开始前打印本轮参数（方法/规模/并行度）
  CANDY_LOG_INFO("TEST", "[BEGIN] method={} size={} parallelism={} ", method, data_size, parallelism);

  static PerfConfigSets g_sets_for_dim = loadPerfConfig();
  TestDataGenerator::Config config; config.vector_dim = g_sets_for_dim.vector_dim; config.similarity_threshold = 0.8;
  config.time_interval = g_sets_for_dim.time_interval_ms;
  // 为了保证总记录数严格等于 data_size：
  // 令目标比例为：pos=10%, near=0%, neg=60%, tail=30%（以记录数计）
  // 其中 pos/near/neg 为“成对”产生记录，因此 pairs = floor((比例*data_size)/2)
  {
    int target_pos = static_cast<int>(data_size * 0.10);
    int target_near = 0; // 如需使用阈值邻近样本，可在配置中开启并计算为 floor(0.05*N)/2 等
    int target_neg = static_cast<int>(data_size * 0.60);
    int pos_pairs = target_pos / 2;
    int near_pairs = target_near / 2;
    int neg_pairs = target_neg / 2;
    int used = 2*pos_pairs + 2*near_pairs + 2*neg_pairs;
    int tail = std::max(0, data_size - used);
    config.positive_pairs = pos_pairs;
    config.near_threshold_pairs = near_pairs;
    config.negative_pairs = neg_pairs;
    config.random_tail = tail;
  }
  config.seed = 42;

  TestDataGenerator generator(config);
  auto [records, _expected_single_stream] = generator.generateData();

  // 仅使用 perf_join.toml 的窗口与阈值配置（不再读取其他 toml）
  static PerfConfigSets g_sets = loadPerfConfig();
  uint64_t trig_ms = g_sets.trig_ms; double threshold_override = g_sets.threshold;

  // 打印本轮的窗口/阈值等关键参数
  CANDY_LOG_INFO("TEST", "[PARAM] threshold={} win_ms={} trig_ms={} time_interval_ms={} ", threshold_override, win_ms, trig_ms, g_sets.time_interval_ms);

  // 构建环境与 Source
  StreamEnvironment env;
  JoinMetrics::instance().reset();

  // 分裂左右流并保证右侧 UID 偏移
  std::vector<std::unique_ptr<VectorRecord>> left_records;
  left_records.reserve(records.size());
  for (auto &r: records) {
    left_records.push_back(std::move(r));
  }
  std::vector<std::unique_ptr<VectorRecord>> right_records;
  right_records.reserve(left_records.size());
  constexpr uint64_t kRightUidOffset = 500000;
  for (auto &lr : left_records) {
    // 构造新对象以替换 UID（避免直接修改 const 成员）
    right_records.push_back(std::make_unique<VectorRecord>(lr->uid_ + kRightUidOffset, lr->timestamp_, lr->data_));
  }
  // 记录期望输入计数（左右流）用于等待处理完成
  const size_t expected_left = left_records.size();
  const size_t expected_right = right_records.size();

  // 基于滑动窗口遍历计算期望匹配集（严格遵循窗口与相似度阈值）
  auto expected_matches = computeExpectedPairsByTraversal(left_records, right_records, threshold_override, win_ms);

  auto left_source = std::make_shared<TestVectorStreamSource>("PerfLeft", std::move(left_records));
  auto right_source = std::make_shared<TestVectorStreamSource>("PerfRight", std::move(right_records));

  // JoinFunction with window from config / override
  auto join_func = std::make_unique<JoinFunction>(
      "SimpleJoin",
      [](std::unique_ptr<VectorRecord>& left,
         std::unique_ptr<VectorRecord>& right) -> std::unique_ptr<VectorRecord> {
        auto lv = extractFloatVector(*left);
        auto rv = extractFloatVector(*right);
        std::vector<float> out; out.reserve(lv.size()+rv.size());
        out.insert(out.end(), lv.begin(), lv.end());
        out.insert(out.end(), rv.begin(), rv.end());
        uint64_t id = left->uid_ * 1000000 + right->uid_ % 1000000; // 保持编码一致
        int64_t ts = std::max(left->timestamp_, right->timestamp_);
        return createVectorRecord(id, ts, out);
    }, g_sets.vector_dim);
  join_func->setWindow(win_ms, trig_ms);

  // pipeline 构建
  // 收集实际匹配对用于精准 recall
  std::mutex match_mutex; std::unordered_set<std::pair<uint64_t,uint64_t>, PairHash> actual_pairs;
  auto sink_func = std::make_unique<SinkFunction>("PerfSink", [&](std::unique_ptr<VectorRecord>& rec){
      if (!rec) {
        return;
      }
      uint64_t cid = rec->uid_;
      uint64_t lid = cid / 1000000;
      uint64_t rid = cid % 1000000;
      std::lock_guard<std::mutex> lg(match_mutex);
      actual_pairs.insert({lid, rid});
  });

  left_source->join(right_source, std::move(join_func), method, threshold_override, (size_t)parallelism)
             ->writeSink(std::move(sink_func), 1);

  env.addStream(left_source);
  env.addStream(right_source);

  auto start_time = std::chrono::high_resolution_clock::now();
  env.execute();
  // 等待直到 JoinOperator 消费完所有输入（以指标计数为准），避免固定时间等待
  {
    using namespace std::chrono_literals;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(800); // 最长等待 800s
    for (;;) {
      uint64_t l = JoinMetrics::instance().total_records_left.load();
      uint64_t r = JoinMetrics::instance().total_records_right.load();
      if (l >= expected_left && r >= expected_right) break;
      if (std::chrono::steady_clock::now() >= deadline) {
        CANDY_LOG_WARN("TEST", "wait_for_processed timeout l={}/{} r={}/{}", l, expected_left, r, expected_right);
        break;
      }
      std::this_thread::sleep_for(5ms);
    }
    // 再等待输出稳定（total_emits 在短时间窗口内不再增长），避免在有在途计算时过早 stop
    {
      const auto stable_window = 50ms; // 连续 50ms 无增长视为稳定
      const auto max_wait = std::chrono::seconds(5);
      uint64_t last = JoinMetrics::instance().total_emits.load();
      auto stable_since = std::chrono::steady_clock::now();
      auto end_by = std::chrono::steady_clock::now() + max_wait;
      while (std::chrono::steady_clock::now() < end_by) {
        std::this_thread::sleep_for(5ms);
        uint64_t cur = JoinMetrics::instance().total_emits.load();
        if (cur != last) { last = cur; stable_since = std::chrono::steady_clock::now(); }
        if (std::chrono::steady_clock::now() - stable_since >= stable_window) break;
      }
    }
  }
  env.stop();
  env.awaitTermination();
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

  // 精准匹配统计
  size_t match_count = 0;
  for (auto ap : actual_pairs) {
    // CANDY_LOG_INFO("TEST", "  Actual match: L={} R={} ", ap.first, ap.second);
    if (expected_matches.count(ap)) match_count++;
  }
  for (auto ep : expected_matches) {
    // CANDY_LOG_INFO("TEST", "  Expected match: L={} R={} ", ep.first, ep.second);
  }

  double recall = expected_matches.empty() ? 1.0 : static_cast<double>(match_count) / static_cast<double>(expected_matches.size());

  double precision = static_cast<double>(match_count)/static_cast<double>(actual_pairs.size());
  double f1 = (precision+recall)>0 ? 2*precision*recall/(precision+recall):0.0;

  CANDY_LOG_INFO("TEST", "Method={} Size={} Parallelism={} time_ms={} matches={} expected={} recall={} precision={} f1={} win_ms={} trig_ms={} ",
                 method, data_size, parallelism, duration.count(), match_count, expected_matches.size(), recall, precision, f1, win_ms, trig_ms);

  // 将结果追加写入报告
  try {
    const auto report_dir =
#ifdef PROJECT_DIR
      std::filesystem::path(PROJECT_DIR) / "test" / "result"
#else
      std::filesystem::current_path() / "test" / "result"
#endif
    ;
    std::filesystem::create_directories(report_dir);
    const auto report_path_fs = report_dir / "perf_report.tsv";
    std::string report_path = report_path_fs.string();
    bool new_file = !std::filesystem::exists(report_path);
    std::ofstream ofs(report_path, std::ios::app);
    if (!ofs.is_open()) {
      CANDY_LOG_WARN("TEST", "[REPORT] cannot open {} for write", report_path);
      return; // 放弃写报告，但不影响测试断言
    }
    if (new_file) {
      ofs << "method\tsize\tparallelism\ttime_ms\tmatches\texpected\trecall\tprecision\tf1\twin_ms\ttrig_ms\t"
             "lock_wait_ms\twindow_ns\tindex_ns\tsim_ns\tjoinF_ns\temit_ns\tcandidate_fetch_ns\t"
             "input_tput_rps\toutput_tput_rps\tavg_apply_ms\tavg_e2e_ms\n";
    }
    uint64_t lock_wait_ms = JoinMetrics::instance().lock_wait_ns.load()/1000000;
    // 吞吐量指标：输入为左右流总记录数，输出为总发射数；均以执行时长换算为每秒
    double elapsed_sec = std::max(1e-6, duration.count()/1000.0);
    uint64_t total_in = JoinMetrics::instance().total_records_left.load() + JoinMetrics::instance().total_records_right.load();
    uint64_t total_out = JoinMetrics::instance().total_emits.load();
    double input_tput_rps = total_in / elapsed_sec;
    double output_tput_rps = total_out / elapsed_sec;
    // 延迟指标（平均）：apply 处理与端到端
    double avg_apply_ms = 0.0;
    {
      uint64_t c = JoinMetrics::instance().apply_processing_count.load();
      if (c > 0) avg_apply_ms = (JoinMetrics::instance().apply_processing_ns.load() / 1e6) / (double)c;
    }
    double avg_e2e_ms = 0.0;
    {
      uint64_t c = JoinMetrics::instance().e2e_latency_count.load();
      if (c > 0) avg_e2e_ms = (JoinMetrics::instance().e2e_latency_ns.load() / 1e6) / (double)c;
    }
    ofs << method << '\t' << data_size << '\t' << parallelism << '\t' << duration.count() << '\t'
        << match_count << '\t' << expected_matches.size() << '\t' << recall << '\t' << precision << '\t' << f1 << '\t'
        << win_ms << '\t' << trig_ms << '\t' << lock_wait_ms << '\t'
        << JoinMetrics::instance().window_insert_ns.load() << '\t'
        << JoinMetrics::instance().index_insert_ns.load() << '\t'
        << JoinMetrics::instance().similarity_ns.load() << '\t'
        << JoinMetrics::instance().join_function_ns.load() << '\t'
        << JoinMetrics::instance().emit_ns.load() << '\t'
    << JoinMetrics::instance().candidate_fetch_ns.load() << '\t'
        << input_tput_rps << '\t' << output_tput_rps << '\t' << avg_apply_ms << '\t' << avg_e2e_ms << '\n';
  ofs.flush();
    CANDY_LOG_INFO("TEST", "[REPORT] appended to {}", report_path);
  } catch(const std::exception &e) {
    CANDY_LOG_WARN("TEST", "write_report_failed what={} ", e.what());
  }

  // 基本性能与锁争用检测
  EXPECT_LT(duration.count(), data_size * g_sets_for_dim.vector_dim) << "Performance too slow for method " << method;
  uint64_t total_time_ns = JoinMetrics::instance().window_insert_ns.load() +
                          JoinMetrics::instance().index_insert_ns.load() +
                          JoinMetrics::instance().similarity_ns.load() +
                          JoinMetrics::instance().join_function_ns.load();
  if (total_time_ns > 0) {
    // double lock_ratio = (double)JoinMetrics::instance().lock_wait_ns.load() / (double)total_time_ns;
    // EXPECT_LE(lock_ratio, 0.4) << "Lock contention too high: " << lock_ratio * 100 << "%";
  }
}

// 动态实例化：读取配置文件 sets 生成参数
static std::vector<PerfCaseParam> buildParams() {
  static PerfConfigSets sets = loadPerfConfig();
  std::vector<PerfCaseParam> params;
  for (auto &m : sets.methods)
    for (auto sz : sets.sizes)
      for (auto par : sets.parallelism)
        for (auto win : sets.win_ms_list)
        {
          CANDY_LOG_INFO("TEST", "[PARAMGEN] method={} size={} parallelism={} win_ms={} ", m, sz, par, win);
          params.push_back({m, sz, par, win});
        }
  return params;
}

INSTANTIATE_TEST_SUITE_P(JoinPerformanceTests, JoinScalingTest,
  ::testing::ValuesIn(buildParams()));

TEST_F(JoinPerformanceTest, MethodSpeedComparison) {
  // 从配置读取 sizes、parallelism、methods 以及窗口与阈值
  auto sets = loadPerfConfig();

  for (auto data_size : sets.sizes) {
    for (auto par : sets.parallelism) {
      for (auto win_ms : sets.win_ms_list) {
        CANDY_LOG_INFO("TEST", "[BEGIN] MethodSpeedComparison size={} parallelism={} win_ms={} ", data_size, par, win_ms);
        CANDY_LOG_INFO("TEST", "[PARAM] threshold={} win_ms={} trig_ms={} ", sets.threshold, win_ms, sets.trig_ms);

      std::vector<std::pair<std::string, int64_t>> method_times;

      for (const auto& method : sets.methods) {
        // 为当前方法生成确定性数据（按 data_size 严格对齐）
  TestDataGenerator::Config cfg; cfg.vector_dim = sets.vector_dim; cfg.similarity_threshold = sets.threshold; cfg.seed = 42; cfg.time_interval = sets.time_interval_ms;
        {
          int target_pos = static_cast<int>(data_size * 0.10);
          int target_near = 0; // 可按需开启近邻样本
          int target_neg = static_cast<int>(data_size * 0.60);
          int pos_pairs = target_pos / 2;
          int near_pairs = target_near / 2;
          int neg_pairs = target_neg / 2;
          int used = 2*pos_pairs + 2*near_pairs + 2*neg_pairs;
          int tail = std::max(0, data_size - used);
          cfg.positive_pairs = pos_pairs;
          cfg.near_threshold_pairs = near_pairs;
          cfg.negative_pairs = neg_pairs;
          cfg.random_tail = tail;
        }

        TestDataGenerator gen(cfg);
        auto [records, expected_matches] = gen.generateData();

        // 切分左右流，右侧UID偏移，基于流式管道进行计时（以体现并行度）
        std::vector<std::unique_ptr<VectorRecord>> left_records;
        left_records.reserve(records.size());
        for (auto &r : records) left_records.push_back(std::move(r));

        std::vector<std::unique_ptr<VectorRecord>> right_records;
        right_records.reserve(left_records.size());
        constexpr uint64_t kRightUidOffset = 500000;
        for (auto &lr : left_records) {
          right_records.push_back(std::make_unique<VectorRecord>(lr->uid_ + kRightUidOffset, lr->timestamp_, lr->data_));
        }

  // 记录期望输入计数后再 move 到 Source
  const size_t expected_left = left_records.size();
  const size_t expected_right = right_records.size();
  auto left_source = std::make_shared<TestVectorStreamSource>("MSLeft", std::move(left_records));
  auto right_source = std::make_shared<TestVectorStreamSource>("MSRight", std::move(right_records));

  auto join_func = createSimpleJoinFunction(sets.vector_dim, win_ms, sets.trig_ms);

        std::atomic<size_t> match_count{0};
        auto sink_func = std::make_unique<SinkFunction>("MSSink", [&](std::unique_ptr<VectorRecord>& rec){ if (rec) match_count++; });

        StreamEnvironment env;
        JoinMetrics::instance().reset();

  left_source->join(right_source, std::move(join_func), method, sets.threshold, (size_t)par)
                   ->writeSink(std::move(sink_func), 1);
        env.addStream(left_source);
        env.addStream(right_source);

        auto start = std::chrono::high_resolution_clock::now();
        env.execute();
        // 等待直到 JoinOperator 消费完所有输入（以指标计数为准）
        {
          using namespace std::chrono_literals;
          const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
          for (;;) {
            uint64_t l = JoinMetrics::instance().total_records_left.load();
            uint64_t r = JoinMetrics::instance().total_records_right.load();
            if (l >= expected_left && r >= expected_right) break;
            if (std::chrono::steady_clock::now() >= deadline) {
              CANDY_LOG_WARN("TEST", "wait_for_processed timeout l={}/{} r={}/{}", l, expected_left, r, expected_right);
              break;
            }
            std::this_thread::sleep_for(5ms);
          }
        }
        env.stop();
        env.awaitTermination();
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  method_times.emplace_back(method, duration.count());
  CANDY_LOG_INFO("TEST", "Method={} Size={} Par={} time_ms={} matches={} win_ms={} trig_ms={} ",
           method, data_size, par, duration.count(), (size_t)match_count.load(), win_ms, sets.trig_ms);
      }

      // 验证：在较大规模（>=5000）下 IVF 应当快于 Bruteforce（放宽倍数关系以避免偶然波动）
      auto bruteforce_time = std::find_if(method_times.begin(), method_times.end(),
          [](const auto& p) { return p.first == "bruteforce_eager"; });
      auto ivf_time = std::find_if(method_times.begin(), method_times.end(),
          [](const auto& p) { return p.first == "ivf_eager"; });

      if (data_size >= 5000 && bruteforce_time != method_times.end() && ivf_time != method_times.end()) {
        EXPECT_LT(ivf_time->second * 2, bruteforce_time->second)
          << "IVF should be faster than BruteForce for large datasets (size=" << data_size << ", par=" << par << ")";
      }
      }
    }
  }
}

} // namespace test
} // namespace candy
