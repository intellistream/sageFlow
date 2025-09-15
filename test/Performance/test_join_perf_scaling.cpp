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
  uint64_t win_ms{2000};
  uint64_t trig_ms{500};
  int vector_dim{128};
};

struct PerfCaseParam { std::string method; int size; int parallelism; };

inline PerfConfigSets loadPerfConfig() {
  PerfConfigSets out; 
  // 使用新的动态配置系统
  std::vector<DynamicConfig> perf_configs;
  if (DynamicConfigManager::loadConfigs("config/perf_join.toml", "performance_test", perf_configs) && !perf_configs.empty()) {
    // 目前只取第一个配置块，若需要可扩展成多组
    const auto& config = perf_configs.front();
    
  out.threshold = config.get<double>("similarity_threshold", out.threshold);
  // 注意：DynamicConfig 将整数优先存为 int，因此这里按 int 读取以避免类型不匹配导致默认值生效
  out.win_ms = static_cast<uint64_t>(config.get<int>("window_time_ms", static_cast<int>(out.win_ms)));
    
  auto trigger_ms = config.get<int>("window_trigger_ms", 0);
  if (trigger_ms > 0) out.trig_ms = static_cast<uint64_t>(trigger_ms);
    
    out.methods = config.get<std::vector<std::string>>("methods", out.methods);
    out.parallelism = config.get<std::vector<int>>("parallelism", out.parallelism);
  out.vector_dim = config.get<int>("vector_dim", out.vector_dim);
    
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
    ss<<"] threshold="<<out.threshold<<" win_ms="<<out.win_ms<<" trig_ms="<<out.trig_ms<<" vector_dim="<<out.vector_dim;
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
static std::unordered_set<std::pair<uint64_t,uint64_t>, PairHash>
computeExpectedCrossStreamPairs(const std::vector<std::unique_ptr<VectorRecord>>& left_records,
                                const TestDataGenerator::Config& gen_cfg,
                                uint64_t right_uid_offset,
                                uint64_t window_ms,
                                uint64_t modulo_base = 1000000ULL) {
  std::unordered_set<std::pair<uint64_t,uint64_t>, PairHash> expected;
  expected.reserve(left_records.size() * 2);

  // 1) 自匹配：每个左记录与其右侧复制体
  for (const auto& lr : left_records) {
    uint64_t lid = lr->uid_;
    uint64_t rid = (lid + right_uid_offset) % modulo_base;
    expected.insert({lid, rid});
  }

  // 2) 正样本对（位于开头的 2*positive_pairs 条，成对排列），需在时间窗口内
  int pos_pairs = gen_cfg.positive_pairs;
  for (int k = 0; k < pos_pairs; ++k) {
    size_t i = static_cast<size_t>(2*k);
    size_t j = i + 1;
    if (j < left_records.size()) {
      const auto& li = left_records[i];
      const auto& lj = left_records[j];
      int64_t dt = (li->timestamp_ > lj->timestamp_) ? (li->timestamp_ - lj->timestamp_) : (lj->timestamp_ - li->timestamp_);
      if (dt <= static_cast<int64_t>(window_ms)) {
        uint64_t u1 = li->uid_;
        uint64_t u2 = lj->uid_;
        expected.insert({u1, (u2 + right_uid_offset) % modulo_base});
        expected.insert({u2, (u1 + right_uid_offset) % modulo_base});
      }
    }
  }

  // 3) near-threshold 对：偶数下标(i%2==0)目标相似度略高于阈值，且需在时间窗口内 -> 计入跨流匹配
  int near_pairs = gen_cfg.near_threshold_pairs;
  size_t near_base = static_cast<size_t>(2 * pos_pairs);
  for (int i = 0; i < near_pairs; ++i) {
    size_t a = near_base + static_cast<size_t>(2*i);
    size_t b = a + 1;
    if (b < left_records.size()) {
      if ((i % 2) == 0) {
        const auto& la = left_records[a];
        const auto& lb = left_records[b];
        int64_t dt = (la->timestamp_ > lb->timestamp_) ? (la->timestamp_ - lb->timestamp_) : (lb->timestamp_ - la->timestamp_);
        if (dt <= static_cast<int64_t>(window_ms)) {
          uint64_t u1 = la->uid_;
          uint64_t u2 = lb->uid_;
          expected.insert({u1, (u2 + right_uid_offset) % modulo_base});
          expected.insert({u2, (u1 + right_uid_offset) % modulo_base});
        }
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
  auto method = p.method; int data_size = p.size; int parallelism = p.parallelism;

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
  uint64_t win_ms = g_sets.win_ms; uint64_t trig_ms = g_sets.trig_ms; double threshold_override = g_sets.threshold;

  // 打印本轮的窗口/阈值等关键参数
  CANDY_LOG_INFO("TEST", "[PARAM] threshold={} win_ms={} trig_ms={} ", threshold_override, win_ms, trig_ms);

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

  // 基于当前两流 join 语义计算期望匹配集
  auto expected_matches = computeExpectedCrossStreamPairs(left_records, config, kRightUidOffset, win_ms);

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
  // 等待数据完全处理（经验性等待：窗口触发 + 处理）
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  env.stop();
  env.awaitTermination();
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

  // 精准匹配统计
  size_t match_count = actual_pairs.size();
  double recall = expected_matches.empty() ? 1.0 : (double)match_count / (double)expected_matches.size();

  // precision = true_positive / actual; 这里所有 actual_pairs 视为预测对
  size_t true_positive = 0; for (auto &pr : actual_pairs) if (expected_matches.count(pr)) true_positive++;
  double precision = match_count==0?1.0:(double)true_positive/(double)match_count;
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
      ofs << "method\tsize\tparallelism\ttime_ms\tmatches\texpected\trecall\tprecision\tf1\twin_ms\ttrig_ms\tlock_wait_ms\twindow_ns\tindex_ns\tsim_ns\tjoinF_ns\temit_ns\tcandidate_fetch_ns\n";
    }
    uint64_t lock_wait_ms = JoinMetrics::instance().lock_wait_ns.load()/1000000;
    ofs << method << '\t' << data_size << '\t' << parallelism << '\t' << duration.count() << '\t'
        << match_count << '\t' << expected_matches.size() << '\t' << recall << '\t' << precision << '\t' << f1 << '\t'
        << win_ms << '\t' << trig_ms << '\t' << lock_wait_ms << '\t'
        << JoinMetrics::instance().window_insert_ns.load() << '\t'
        << JoinMetrics::instance().index_insert_ns.load() << '\t'
        << JoinMetrics::instance().similarity_ns.load() << '\t'
        << JoinMetrics::instance().join_function_ns.load() << '\t'
        << JoinMetrics::instance().emit_ns.load() << '\t'
    << JoinMetrics::instance().candidate_fetch_ns.load() << '\n';
  ofs.flush();
    CANDY_LOG_INFO("TEST", "[REPORT] appended to {}", report_path);
  } catch(const std::exception &e) {
    CANDY_LOG_WARN("TEST", "write_report_failed what={} ", e.what());
  }

  // 基本性能与锁争用检测
  EXPECT_LT(duration.count(), data_size * 100) << "Performance too slow for method " << method;
  uint64_t total_time_ns = JoinMetrics::instance().window_insert_ns.load() +
                          JoinMetrics::instance().index_insert_ns.load() +
                          JoinMetrics::instance().similarity_ns.load() +
                          JoinMetrics::instance().join_function_ns.load();
  if (total_time_ns > 0) {
    double lock_ratio = (double)JoinMetrics::instance().lock_wait_ns.load() / (double)total_time_ns;
    EXPECT_LE(lock_ratio, 0.4) << "Lock contention too high: " << lock_ratio * 100 << "%";
  }
}

// 动态实例化：读取配置文件 sets 生成参数
static std::vector<PerfCaseParam> buildParams() {
  static PerfConfigSets sets = loadPerfConfig();
  std::vector<PerfCaseParam> params;
  for (auto &m : sets.methods)
    for (auto sz : sets.sizes)
      for (auto par : sets.parallelism)
        {
          CANDY_LOG_INFO("TEST", "[PARAMGEN] method={} size={} parallelism={} ", m, sz, par);
          params.push_back({m, sz, par});
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
      CANDY_LOG_INFO("TEST", "[BEGIN] MethodSpeedComparison size={} parallelism={} ", data_size, par);
      CANDY_LOG_INFO("TEST", "[PARAM] threshold={} win_ms={} trig_ms={} ", sets.threshold, sets.win_ms, sets.trig_ms);

      std::vector<std::pair<std::string, int64_t>> method_times;

      for (const auto& method : sets.methods) {
        // 为当前方法生成确定性数据（按 data_size 严格对齐）
        TestDataGenerator::Config cfg; cfg.vector_dim = sets.vector_dim; cfg.similarity_threshold = sets.threshold; cfg.seed = 42;
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

        auto left_source = std::make_shared<TestVectorStreamSource>("MSLeft", std::move(left_records));
        auto right_source = std::make_shared<TestVectorStreamSource>("MSRight", std::move(right_records));

  auto join_func = createSimpleJoinFunction(sets.vector_dim, sets.win_ms, sets.trig_ms);

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
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        env.stop();
        env.awaitTermination();
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  method_times.emplace_back(method, duration.count());
  CANDY_LOG_INFO("TEST", "Method={} Size={} Par={} time_ms={} matches={} win_ms={} trig_ms={} ",
           method, data_size, par, duration.count(), (size_t)match_count.load(), sets.win_ms, sets.trig_ms);
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

} // namespace test
} // namespace candy
