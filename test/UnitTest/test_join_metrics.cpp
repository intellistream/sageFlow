/**
 * @file test_join_metrics.cpp
 * @brief Unit tests for Join metrics collection and validation system
 */

#include <gtest/gtest.h>

#include <chrono>
#include <thread>
#include <vector>

#include "metrics/join_metrics_collector.h"
#include "metrics/metrics_validator.h"

using namespace sageFlow::metrics;

// ============================================================================
// JoinExecutionStats Tests
// ============================================================================

class JoinExecutionStatsTest : public ::testing::Test {
  protected:
    JoinExecutionStats stats;
};

TEST_F(JoinExecutionStatsTest, RecallCalculation) {
    stats.true_positives = 80;
    stats.false_negatives = 20;

    EXPECT_DOUBLE_EQ(stats.recall(), 0.8);
}

TEST_F(JoinExecutionStatsTest, RecallZeroWhenNoRelevant) {
    stats.true_positives = 0;
    stats.false_negatives = 0;

    EXPECT_DOUBLE_EQ(stats.recall(), 0.0);
}

TEST_F(JoinExecutionStatsTest, PrecisionCalculation) {
    stats.true_positives = 80;
    stats.false_positives = 20;

    EXPECT_DOUBLE_EQ(stats.precision(), 0.8);
}

TEST_F(JoinExecutionStatsTest, PrecisionZeroWhenNoRetrieved) {
    stats.true_positives = 0;
    stats.false_positives = 0;

    EXPECT_DOUBLE_EQ(stats.precision(), 0.0);
}

TEST_F(JoinExecutionStatsTest, F1ScoreCalculation) {
    stats.true_positives = 80;
    stats.false_positives = 20;
    stats.false_negatives = 20;

    // precision = 0.8, recall = 0.8
    // F1 = 2 * 0.8 * 0.8 / (0.8 + 0.8) = 1.28 / 1.6 = 0.8
    EXPECT_DOUBLE_EQ(stats.f1Score(), 0.8);
}

TEST_F(JoinExecutionStatsTest, F1ScoreZeroWhenNoData) {
    EXPECT_DOUBLE_EQ(stats.f1Score(), 0.0);
}

TEST_F(JoinExecutionStatsTest, ThroughputCalculation) {
    stats.left_records_processed = 500;
    stats.right_records_processed = 500;
    stats.total_time = std::chrono::seconds(1);

    EXPECT_DOUBLE_EQ(stats.throughputRecordsPerSec(), 1000.0);
}

TEST_F(JoinExecutionStatsTest, ThroughputZeroWhenNoTime) {
    stats.left_records_processed = 100;

    EXPECT_DOUBLE_EQ(stats.throughputRecordsPerSec(), 0.0);
}

TEST_F(JoinExecutionStatsTest, AvgQueryTimeCalculation) {
    stats.query_time = std::chrono::microseconds(1000);  // 1000 us
    stats.index_queries = 10;

    EXPECT_DOUBLE_EQ(stats.avgQueryTimeUs(), 100.0);  // 100 us per query
}

// ============================================================================
// JoinMetricsCollector Tests
// ============================================================================

class JoinMetricsCollectorTest : public ::testing::Test {
  protected:
    void SetUp() override { collector = std::make_unique<JoinMetricsCollector>("test_collector"); }

    std::unique_ptr<JoinMetricsCollector> collector;
};

TEST_F(JoinMetricsCollectorTest, ConstructorSetsName) { EXPECT_EQ(collector->name(), "test_collector"); }

TEST_F(JoinMetricsCollectorTest, RecordLeftProcessed) {
    collector->recordLeftProcessed(5);
    collector->recordLeftProcessed(3);

    auto stats = collector->snapshot();
    EXPECT_EQ(stats.left_records_processed, 8);
}

TEST_F(JoinMetricsCollectorTest, RecordRightProcessed) {
    collector->recordRightProcessed(10);

    auto stats = collector->snapshot();
    EXPECT_EQ(stats.right_records_processed, 10);
}

TEST_F(JoinMetricsCollectorTest, RecordComparison) {
    collector->recordComparison(100);
    collector->recordComparison(50);

    auto stats = collector->snapshot();
    EXPECT_EQ(stats.total_comparisons, 150);
}

TEST_F(JoinMetricsCollectorTest, RecordCandidate) {
    collector->recordCandidate(25);

    auto stats = collector->snapshot();
    EXPECT_EQ(stats.candidate_pairs, 25);
}

TEST_F(JoinMetricsCollectorTest, RecordMatch) {
    collector->recordMatch(10);

    auto stats = collector->snapshot();
    EXPECT_EQ(stats.output_matches, 10);
}

TEST_F(JoinMetricsCollectorTest, RecordIndexOperations) {
    collector->recordIndexInsert(5);
    collector->recordIndexDelete(2);
    collector->recordIndexQuery(10);
    collector->recordIndexRebuild();

    auto stats = collector->snapshot();
    EXPECT_EQ(stats.index_inserts, 5);
    EXPECT_EQ(stats.index_deletes, 2);
    EXPECT_EQ(stats.index_queries, 10);
    EXPECT_EQ(stats.index_rebuilds, 1);
}

TEST_F(JoinMetricsCollectorTest, UpdateAccuracyMetrics) {
    collector->updateAccuracyMetrics(80, 10, 10);

    auto stats = collector->snapshot();
    EXPECT_EQ(stats.true_positives, 80);
    EXPECT_EQ(stats.false_positives, 10);
    EXPECT_EQ(stats.false_negatives, 10);
}

TEST_F(JoinMetricsCollectorTest, IncrementalAccuracyRecording) {
    collector->recordTruePositive(5);
    collector->recordTruePositive(5);
    collector->recordFalsePositive(2);
    collector->recordFalseNegative(3);

    auto stats = collector->snapshot();
    EXPECT_EQ(stats.true_positives, 10);
    EXPECT_EQ(stats.false_positives, 2);
    EXPECT_EQ(stats.false_negatives, 3);
}

TEST_F(JoinMetricsCollectorTest, ResetClearsAllStats) {
    collector->recordLeftProcessed(100);
    collector->recordMatch(50);
    collector->updateAccuracyMetrics(80, 10, 10);

    collector->reset();

    auto stats = collector->snapshot();
    EXPECT_EQ(stats.left_records_processed, 0);
    EXPECT_EQ(stats.output_matches, 0);
    EXPECT_EQ(stats.true_positives, 0);
}

TEST_F(JoinMetricsCollectorTest, ScopedTimerCorrectlyMeasures) {
    {
        auto timer = collector->scopedTimer("query");
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    auto stats = collector->snapshot();
    // 应该至少有 10ms 的时间记录
    EXPECT_GE(stats.query_time.count(), 10'000'000);  // >= 10ms in ns
}

TEST_F(JoinMetricsCollectorTest, ManualTimerStartStop) {
    collector->startTimer("total");
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    collector->stopTimer("total");

    auto stats = collector->snapshot();
    EXPECT_GE(stats.total_time.count(), 5'000'000);  // >= 5ms in ns
}

TEST_F(JoinMetricsCollectorTest, MultipleTimerPhases) {
    {
        auto t1 = collector->scopedTimer("index_build");
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    {
        auto t2 = collector->scopedTimer("query");
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    auto stats = collector->snapshot();
    EXPECT_GE(stats.index_build_time.count(), 5'000'000);
    EXPECT_GE(stats.query_time.count(), 5'000'000);
}

TEST_F(JoinMetricsCollectorTest, ThreadSafeUpdates) {
    std::vector<std::thread> threads;
    const int num_threads = 10;
    const int increments_per_thread = 1000;

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([this, increments_per_thread]() {
            for (int j = 0; j < increments_per_thread; ++j) {
                collector->recordLeftProcessed();
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    auto stats = collector->snapshot();
    EXPECT_EQ(stats.left_records_processed, num_threads * increments_per_thread);
}

// ============================================================================
// JoinMetricsRegistry Tests
// ============================================================================

class JoinMetricsRegistryTest : public ::testing::Test {
  protected:
    void SetUp() override {
        // 清空注册表
        JoinMetricsRegistry::instance().clear();
    }

    void TearDown() override {
        // 测试后清理
        JoinMetricsRegistry::instance().clear();
    }
};

TEST_F(JoinMetricsRegistryTest, GetOrCreateReturnsNewCollector) {
    auto collector = JoinMetricsRegistry::instance().getOrCreate("test1");
    ASSERT_NE(collector, nullptr);
    EXPECT_EQ(collector->name(), "test1");
}

TEST_F(JoinMetricsRegistryTest, GetOrCreateReturnsSameCollector) {
    auto c1 = JoinMetricsRegistry::instance().getOrCreate("test1");
    auto c2 = JoinMetricsRegistry::instance().getOrCreate("test1");
    EXPECT_EQ(c1.get(), c2.get());
}

TEST_F(JoinMetricsRegistryTest, GetReturnsNullForNonExistent) {
    auto collector = JoinMetricsRegistry::instance().get("nonexistent");
    EXPECT_EQ(collector, nullptr);
}

TEST_F(JoinMetricsRegistryTest, GetReturnsExistingCollector) {
    auto c1 = JoinMetricsRegistry::instance().getOrCreate("test1");
    auto c2 = JoinMetricsRegistry::instance().get("test1");
    EXPECT_EQ(c1.get(), c2.get());
}

TEST_F(JoinMetricsRegistryTest, GetCollectorNames) {
    JoinMetricsRegistry::instance().getOrCreate("collector_a");
    JoinMetricsRegistry::instance().getOrCreate("collector_b");
    JoinMetricsRegistry::instance().getOrCreate("collector_c");

    auto names = JoinMetricsRegistry::instance().getCollectorNames();
    EXPECT_EQ(names.size(), 3);
}

TEST_F(JoinMetricsRegistryTest, AllSnapshots) {
    auto c1 = JoinMetricsRegistry::instance().getOrCreate("c1");
    auto c2 = JoinMetricsRegistry::instance().getOrCreate("c2");

    c1->recordLeftProcessed(10);
    c2->recordLeftProcessed(20);

    auto snapshots = JoinMetricsRegistry::instance().allSnapshots();
    EXPECT_EQ(snapshots.size(), 2);
    EXPECT_EQ(snapshots["c1"].left_records_processed, 10);
    EXPECT_EQ(snapshots["c2"].left_records_processed, 20);
}

TEST_F(JoinMetricsRegistryTest, ResetAll) {
    auto c1 = JoinMetricsRegistry::instance().getOrCreate("c1");
    c1->recordLeftProcessed(100);

    JoinMetricsRegistry::instance().resetAll();

    auto stats = c1->snapshot();
    EXPECT_EQ(stats.left_records_processed, 0);
}

TEST_F(JoinMetricsRegistryTest, Remove) {
    JoinMetricsRegistry::instance().getOrCreate("to_remove");
    EXPECT_TRUE(JoinMetricsRegistry::instance().remove("to_remove"));
    EXPECT_EQ(JoinMetricsRegistry::instance().get("to_remove"), nullptr);
}

TEST_F(JoinMetricsRegistryTest, Clear) {
    JoinMetricsRegistry::instance().getOrCreate("c1");
    JoinMetricsRegistry::instance().getOrCreate("c2");

    JoinMetricsRegistry::instance().clear();

    EXPECT_TRUE(JoinMetricsRegistry::instance().getCollectorNames().empty());
}

// ============================================================================
// MetricsValidator Tests
// ============================================================================

class MetricsValidatorTest : public ::testing::Test {
  protected:
    JoinExecutionStats stats;
};

TEST_F(MetricsValidatorTest, RecallThresholdPasses) {
    stats.true_positives = 85;
    stats.false_negatives = 15;  // recall = 0.85

    auto rule = MetricsValidator::recallThreshold(0.80);
    EXPECT_TRUE(rule.check(stats));
}

TEST_F(MetricsValidatorTest, RecallThresholdFails) {
    stats.true_positives = 70;
    stats.false_negatives = 30;  // recall = 0.70

    auto rule = MetricsValidator::recallThreshold(0.80);
    EXPECT_FALSE(rule.check(stats));
}

TEST_F(MetricsValidatorTest, RecallThresholdSkipsWhenNoData) {
    // No data - should pass
    auto rule = MetricsValidator::recallThreshold(0.90);
    EXPECT_TRUE(rule.check(stats));
}

TEST_F(MetricsValidatorTest, PrecisionThresholdPasses) {
    stats.true_positives = 90;
    stats.false_positives = 10;  // precision = 0.90

    auto rule = MetricsValidator::precisionThreshold(0.85);
    EXPECT_TRUE(rule.check(stats));
}

TEST_F(MetricsValidatorTest, PrecisionThresholdFails) {
    stats.true_positives = 50;
    stats.false_positives = 50;  // precision = 0.50

    auto rule = MetricsValidator::precisionThreshold(0.80);
    EXPECT_FALSE(rule.check(stats));
}

TEST_F(MetricsValidatorTest, F1ThresholdPasses) {
    stats.true_positives = 80;
    stats.false_positives = 20;
    stats.false_negatives = 20;  // F1 = 0.8

    auto rule = MetricsValidator::f1Threshold(0.75);
    EXPECT_TRUE(rule.check(stats));
}

TEST_F(MetricsValidatorTest, ThroughputThresholdPasses) {
    stats.left_records_processed = 10000;
    stats.right_records_processed = 10000;
    stats.total_time = std::chrono::seconds(1);  // 20000 records/sec

    auto rule = MetricsValidator::throughputThreshold(10000.0);
    EXPECT_TRUE(rule.check(stats));
}

TEST_F(MetricsValidatorTest, ThroughputThresholdFails) {
    stats.left_records_processed = 100;
    stats.total_time = std::chrono::seconds(1);  // 100 records/sec

    auto rule = MetricsValidator::throughputThreshold(1000.0);
    EXPECT_FALSE(rule.check(stats));
}

TEST_F(MetricsValidatorTest, AvgQueryLatencyThresholdPasses) {
    stats.query_time = std::chrono::microseconds(500);  // 500 us total
    stats.index_queries = 10;                           // 50 us per query

    auto rule = MetricsValidator::avgQueryLatencyThreshold(100.0);  // max 100 us
    EXPECT_TRUE(rule.check(stats));
}

TEST_F(MetricsValidatorTest, AvgQueryLatencyThresholdFails) {
    stats.query_time = std::chrono::microseconds(2000);  // 2000 us total
    stats.index_queries = 10;                            // 200 us per query

    auto rule = MetricsValidator::avgQueryLatencyThreshold(100.0);  // max 100 us
    EXPECT_FALSE(rule.check(stats));
}

TEST_F(MetricsValidatorTest, MinRecordsProcessedPasses) {
    stats.left_records_processed = 500;
    stats.right_records_processed = 500;

    auto rule = MetricsValidator::minRecordsProcessed(1000);
    EXPECT_TRUE(rule.check(stats));
}

TEST_F(MetricsValidatorTest, MinOutputMatchesPasses) {
    stats.output_matches = 100;

    auto rule = MetricsValidator::minOutputMatches(50);
    EXPECT_TRUE(rule.check(stats));
}

TEST_F(MetricsValidatorTest, ValidatorWithMultipleRules) {
    MetricsValidator validator;
    validator.addRule(MetricsValidator::recallThreshold(0.80));
    validator.addRule(MetricsValidator::precisionThreshold(0.80));

    stats.true_positives = 85;
    stats.false_positives = 15;
    stats.false_negatives = 15;
    // recall = 85/100 = 0.85, precision = 85/100 = 0.85

    auto result = validator.validate(stats);
    EXPECT_TRUE(result.passed);
    EXPECT_FALSE(result.hasErrors());
}

TEST_F(MetricsValidatorTest, ValidatorReportsAllFailures) {
    MetricsValidator validator;
    validator.addRule(MetricsValidator::recallThreshold(0.90));
    validator.addRule(MetricsValidator::precisionThreshold(0.90));

    stats.true_positives = 70;
    stats.false_positives = 30;
    stats.false_negatives = 30;
    // recall = 70/100 = 0.70, precision = 70/100 = 0.70

    auto result = validator.validate(stats);
    EXPECT_FALSE(result.passed);
    EXPECT_EQ(result.errors.size(), 2);
}

TEST_F(MetricsValidatorTest, WarningLevelRuleDoesNotFailValidation) {
    MetricsValidator validator;
    validator.addRule(MetricsValidator::recallThreshold(0.90, true));  // warning

    stats.true_positives = 70;
    stats.false_negatives = 30;  // recall = 0.70

    auto result = validator.validate(stats);
    EXPECT_TRUE(result.passed);  // Still passes
    EXPECT_TRUE(result.hasWarnings());
    EXPECT_EQ(result.warnings.size(), 1);
}

TEST_F(MetricsValidatorTest, ValidateOrThrowThrowsOnError) {
    MetricsValidator validator;
    validator.addRule(MetricsValidator::recallThreshold(0.90));

    stats.true_positives = 70;
    stats.false_negatives = 30;

    EXPECT_THROW(validator.validateOrThrow(stats), ValidationException);
}

TEST_F(MetricsValidatorTest, ValidateOrThrowDoesNotThrowOnPass) {
    MetricsValidator validator;
    validator.addRule(MetricsValidator::recallThreshold(0.60));

    stats.true_positives = 70;
    stats.false_negatives = 30;

    EXPECT_NO_THROW(validator.validateOrThrow(stats));
}

TEST_F(MetricsValidatorTest, CreateDefaultValidator) {
    auto validator = MetricsValidator::createDefault();
    EXPECT_GT(validator.ruleCount(), 0);
}

// ============================================================================
// MetricsThresholds Tests
// ============================================================================

class MetricsThresholdsTest : public ::testing::Test {};

TEST_F(MetricsThresholdsTest, DefaultForTestingHasReasonableValues) {
    auto thresholds = MetricsThresholds::defaultForTesting();

    EXPECT_TRUE(thresholds.recall_enabled);
    EXPECT_GT(thresholds.min_recall, 0.0);
    EXPECT_TRUE(thresholds.precision_enabled);
    EXPECT_GT(thresholds.min_precision, 0.0);
}

TEST_F(MetricsThresholdsTest, CreateValidatorFromThresholds) {
    MetricsThresholds thresholds;
    thresholds.min_recall = 0.85;
    thresholds.recall_enabled = true;
    thresholds.min_precision = 0.90;
    thresholds.precision_enabled = true;

    auto validator = thresholds.createValidator();
    EXPECT_EQ(validator.ruleCount(), 2);
}

TEST_F(MetricsThresholdsTest, DisabledThresholdsNotIncluded) {
    MetricsThresholds thresholds;
    thresholds.min_recall = 0.85;
    thresholds.recall_enabled = false;  // disabled

    auto validator = thresholds.createValidator();
    EXPECT_EQ(validator.ruleCount(), 0);
}

// ============================================================================
// ValidationResult Tests
// ============================================================================

TEST(ValidationResultTest, SummaryForPassedResult) {
    ValidationResult result;
    result.passed = true;

    auto summary = result.summary();
    EXPECT_TRUE(summary.find("PASSED") != std::string::npos);
}

TEST(ValidationResultTest, SummaryForFailedResult) {
    ValidationResult result;
    result.passed = false;
    result.errors.push_back("Error 1");
    result.errors.push_back("Error 2");

    auto summary = result.summary();
    EXPECT_TRUE(summary.find("FAILED") != std::string::npos);
    EXPECT_TRUE(summary.find("2 error(s)") != std::string::npos);
}

TEST(ValidationResultTest, MergeResults) {
    ValidationResult r1;
    r1.passed = true;
    r1.warnings.push_back("Warning 1");

    ValidationResult r2;
    r2.passed = false;
    r2.errors.push_back("Error 1");

    r1.merge(r2);

    EXPECT_FALSE(r1.passed);
    EXPECT_EQ(r1.errors.size(), 1);
    EXPECT_EQ(r1.warnings.size(), 1);
}

// ============================================================================
// Integration with Global JoinMetrics
// ============================================================================

TEST(GlobalMetricsIntegration, SnapshotFromGlobal) {
    // Reset global metrics first
    sageFlow::JoinMetrics::instance().reset();

    // Record some data
    sageFlow::JoinMetrics::instance().total_records_left.fetch_add(100, std::memory_order_relaxed);
    sageFlow::JoinMetrics::instance().total_records_right.fetch_add(200, std::memory_order_relaxed);
    sageFlow::JoinMetrics::instance().total_emits.fetch_add(50, std::memory_order_relaxed);

    auto stats = JoinMetricsCollector::snapshotFromGlobal();

    EXPECT_EQ(stats.left_records_processed, 100);
    EXPECT_EQ(stats.right_records_processed, 200);
    EXPECT_EQ(stats.output_matches, 50);

    // Clean up
    sageFlow::JoinMetrics::instance().reset();
}
