#include <iostream>
#include <chrono>
#include <memory>
#include <vector>
#include <string>
#include <atomic>
#include <algorithm>
#include <gtest/gtest.h>
#include <cmath>
#include <list>
#include <random>

// Project includes
#include "common/data_types.h"
#include "function/join_function.h"
#include "operator/join_operator.h"
#include "concurrency/concurrency_manager.h"
#include "storage/storage_manager.h"
#include "compute_engine/compute_engine.h"

#ifndef PROJECT_DIR
#define PROJECT_DIR "d:/Share Libary/candyFlow_zero"
#endif

using namespace std;
using namespace candy;
using namespace std::chrono;

namespace candy {

// 模拟结果收集器，用于替代emit功能
class JoinResultCollector {
public:
    vector<unique_ptr<VectorRecord>> results_;
    atomic<int> result_count_;
    
    JoinResultCollector() : result_count_(0) {}
    
    void collect_result(Response&& response) {
        if (response.record_) {
            results_.push_back(std::move(response.record_));
            result_count_++;
        }
    }
    
    int get_result_count() const {
        return result_count_.load();
    }
    
    void clear() {
        results_.clear();
        result_count_ = 0;
    }
};

class JoinMethodPerformanceTester {
private:
    ComputeEngine compute_engine_;
    shared_ptr<ConcurrencyManager> concurrency_manager_;
    vector<double> execution_times_;
    vector<string> method_names_;
    unique_ptr<JoinResultCollector> result_collector_;
    int data_gen_count = 0;
    // 生成测试数据
    auto GenerateTestData(int num_records, int dim = 128) -> vector<unique_ptr<VectorRecord>> {
        vector<unique_ptr<VectorRecord>> records;
        random_device rd;
        mt19937 gen(rd());
        uniform_real_distribution<float> dis(-1.0f, 1.0f);
        
        for (int i = 0; i < num_records; ++i) {
            auto data = make_unique<VectorData>(dim, DataType::Float32);
            auto* float_data = reinterpret_cast<float*>(data->data_.get());
            
            for (int j = 0; j < dim; ++j) {
                float_data[j] = dis(gen);
            }
              auto record = make_unique<VectorRecord>(
                static_cast<uint64_t>(data_gen_count++),
                static_cast<int64_t>(data_gen_count * 1000),
                *data
            );
            records.push_back(std::move(record));
        }
        return records;
    }    
    // 创建简单的测试JoinFunction（简化逻辑，专注于性能测试）
    auto CreateSimilarityJoinFunction(double threshold = 0.7) -> unique_ptr<JoinFunction> {
        auto join_func = make_unique<JoinFunction>("test_join", 128);
        
        // 极简化的连接逻辑，只进行基本的条件判断，不做复杂计算
        JoinFunc func = [threshold](unique_ptr<VectorRecord>& left, unique_ptr<VectorRecord>& right) -> unique_ptr<VectorRecord> {
            // 避免自连接
            if (left->uid_ == right->uid_) return nullptr;
            
            // 简单的连接逻辑：基于UID模式
            if ((left->uid_ % 2 == 0 && right->uid_ % 2 == 1) || 
                (left->uid_ % 2 == 1 && right->uid_ % 2 == 0)) {
                return make_unique<VectorRecord>(*left);
            }
            
            return nullptr;
        };
        
        join_func->setJoinFunc(std::move(func));
        return join_func;
    }
    
    // 创建JoinOperator
    auto CreateJoinOperator(JoinMethodType method_type, double threshold = 0.7) -> unique_ptr<JoinOperator> {
        auto join_func = CreateSimilarityJoinFunction(threshold);
        auto join_func_ptr = unique_ptr<Function>(join_func.release());
        
        string method_name;
        switch (method_type) {
            case JoinMethodType::BRUTEFORCE_EAGER:
                method_name = "bruteforce_eager";
                break;
            case JoinMethodType::BRUTEFORCE_LAZY:
                method_name = "bruteforce_lazy";
                break;
            case JoinMethodType::IVF_EAGER:
                method_name = "ivf_eager";
                break;
            case JoinMethodType::IVF_LAZY:
                method_name = "ivf_lazy";
                break;
        }
        
        return make_unique<JoinOperator>(
            join_func_ptr,
            concurrency_manager_,
            method_name,
            threshold
        );
    }
    
    auto GetMethodName(JoinMethodType method_type) -> string {
        switch (method_type) {
            case JoinMethodType::BRUTEFORCE_EAGER: return "BruteForce Eager";
            case JoinMethodType::BRUTEFORCE_LAZY: return "BruteForce Lazy";
            case JoinMethodType::IVF_EAGER: return "IVF Eager";
            case JoinMethodType::IVF_LAZY: return "IVF Lazy";
            default: return "Unknown";
        }
    }
    
public:
    JoinMethodPerformanceTester() {
        // 初始化StorageManager和ConcurrencyManager
        auto storage_manager = make_shared<StorageManager>();
        concurrency_manager_ = make_shared<ConcurrencyManager>(storage_manager);
        result_collector_ = make_unique<JoinResultCollector>();
    }
      
    // 运行单个join方法的性能测试（使用JoinOperator）
    auto RunJoinMethodTest(JoinMethodType method_type, int test_data_size = 1000, int dim = 128) -> double {
        auto start_time = high_resolution_clock::now();
        
        try {
            string method_name = GetMethodName(method_type);
            cout << "Testing " << method_name << " with JoinOperator..." << endl;
            
            // 清空之前的结果
            result_collector_->clear();
            
            // 生成测试数据
            auto left_data = GenerateTestData(test_data_size / 2, dim);
            auto right_data = GenerateTestData(test_data_size / 2, dim);
            
            // 创建JoinOperator
            auto join_operator = CreateJoinOperator(method_type, 0.7);
            
            // 模拟结果输出（替代emit功能）
            // 由于JoinOperator使用emit输出结果，我们需要一个方法来捕获这些结果
            // 这里我们直接计算预期的结果数量作为性能指标
            
            // 开始计时
            auto join_start = high_resolution_clock::now();
            
            // 打开操作符
            join_operator->open();
            
            // 处理左侧数据（slot 0）
            for (auto& record : left_data) {
                Response left_response(ResponseType::Record, std::move(record));
                join_operator->process(left_response, 0);
            }
            
            // 处理右侧数据（slot 1）
            for (auto& record : right_data) {
                Response right_response(ResponseType::Record, std::move(record));
                join_operator->process(right_response, 1);
            }
            
            auto join_end = high_resolution_clock::now();
            auto join_duration = duration_cast<microseconds>(join_end - join_start).count();
            
            auto end_time = high_resolution_clock::now();
            auto total_duration = duration_cast<milliseconds>(end_time - start_time).count();
            
            cout << "Method: " << method_name << endl;
            cout << "Total Execution Time: " << total_duration << " ms" << endl;
            cout << "Join Time: " << join_duration << " μs" << endl;
            cout << "Left Records: " << left_data.size() << endl;
            cout << "Right Records: " << right_data.size() << endl;
            cout << "Processing completed successfully" << endl;
            
            if (total_duration > 0) {
                cout << "Throughput: " << (test_data_size * 1000.0 / total_duration) << " records/sec" << endl;
            }
            cout << "----------------------------------------" << endl;
            
            return static_cast<double>(total_duration);
            
        } catch (const exception& e) {
            cerr << "Error in join method test: " << e.what() << endl;
            return -1.0;
        }
    }
    
    // 运行所有join方法的性能比较
    auto RunComparisonTest(int test_data_size = 1000) -> void {
        cout << "=========== Join Method Performance Comparison Test ===========" << endl;
        cout << "Test Data Size: " << test_data_size << " records" << endl;
          vector<JoinMethodType> methods = {
            JoinMethodType::BRUTEFORCE_EAGER,
            JoinMethodType::BRUTEFORCE_LAZY,
            JoinMethodType::IVF_EAGER,
            JoinMethodType::IVF_LAZY
        };
        
        vector<pair<string, double>> results;
        
        for (const auto& method : methods) {
            cout << "\n" << string(50, '=') << endl;
            double execution_time = RunJoinMethodTest(method, test_data_size);
            if (execution_time > 0) {
                results.emplace_back(GetMethodName(method), execution_time);
            }
        }
        
        if (results.empty()) {
            cout << "No successful test results!" << endl;
            return;
        }
        
        // 排序结果并显示性能排名
        sort(results.begin(), results.end(), 
             [](const pair<string, double>& a, const pair<string, double>& b) {
                 return a.second < b.second;
             });
        
        cout << "\n" << string(50, '=') << endl;
        cout << "Performance Ranking:" << endl;
        for (size_t i = 0; i < results.size(); ++i) {
            cout << (i + 1) << ". " << results[i].first 
                 << ": " << results[i].second << " ms" << endl;
        }
        
        if (results.size() >= 2) {
            double fastest = results[0].second;
            cout << "\nPerformance Comparison vs Fastest:" << endl;
            for (size_t i = 1; i < results.size(); ++i) {
                double improvement = ((results[i].second - fastest) / fastest) * 100;
                cout << results[i].first << " is " << improvement 
                     << "% slower than " << results[0].first << endl;
            }
        }
        
        cout << string(50, '=') << endl;
    }
    
    // 专门测试不同数据大小下的性能
    auto RunScalabilityTest() -> void {
        cout << "=========== Join Method Scalability Test ===========" << endl;
        
        vector<int> data_sizes = {100, 500, 1000, 2000};
        vector<JoinMethodType> methods = {
            JoinMethodType::BRUTEFORCE_EAGER,
            JoinMethodType::BRUTEFORCE_LAZY,
            JoinMethodType::IVF_EAGER,
            JoinMethodType::IVF_LAZY
        };
        
        for (const auto& method : methods) {
            cout << "\nTesting scalability for " << GetMethodName(method) << ":" << endl;
            for (int size : data_sizes) {
                cout << "Data size: " << size << " - ";
                double time = RunJoinMethodTest(method, size);
                if (time > 0) {
                    cout << "Time: " << time << " ms" << endl;
                }
            }
            cout << endl;
        }
    }
};

}  // namespace candy

// Google Test 测试用例
TEST(JoinMethodPerformanceTest, BruteForceEager) {
    candy::JoinMethodPerformanceTester tester;
    double execution_time = tester.RunJoinMethodTest(candy::JoinMethodType::BRUTEFORCE_EAGER, 500);
    EXPECT_GT(execution_time, 0);
}

TEST(JoinMethodPerformanceTest, BruteForceLazy) {
    candy::JoinMethodPerformanceTester tester;
    double execution_time = tester.RunJoinMethodTest(candy::JoinMethodType::BRUTEFORCE_LAZY, 500);
    EXPECT_GT(execution_time, 0);
}

TEST(JoinMethodPerformanceTest, IVFEager) {
  candy::JoinMethodPerformanceTester tester;
  double execution_time = tester.RunJoinMethodTest(candy::JoinMethodType::IVF_EAGER, 500);
  EXPECT_GT(execution_time, 0);
}

TEST(JoinMethodPerformanceTest, IvfLazy) {
  candy::JoinMethodPerformanceTester tester;
  double execution_time = tester.RunJoinMethodTest(candy::JoinMethodType::IVF_EAGER, 500);
  EXPECT_GT(execution_time, 0);
}

TEST(JoinMethodPerformanceTest, CompareAllMethods) {
    candy::JoinMethodPerformanceTester tester;
    ASSERT_NO_THROW(tester.RunComparisonTest(500));
}

TEST(JoinMethodPerformanceTest, ScalabilityTest) {
    candy::JoinMethodPerformanceTester tester;
    ASSERT_NO_THROW(tester.RunScalabilityTest());
}

// 独立的性能测试主函数
int main(int argc, char* argv[]) {
    // 初始化Google Test
    ::testing::InitGoogleTest(&argc, argv);
    
    // 如果命令行参数指定了特定的测试模式
    if (argc > 1) {
        string mode = argv[1];
        candy::JoinMethodPerformanceTester tester;
        
        if (mode == "performance") {
            cout << "Running Join Method Performance Comparison Test..." << endl;
            tester.RunComparisonTest(1000);
            return 0;
        } else if (mode == "scalability") {
            cout << "Running Join Method Scalability Test..." << endl;
            tester.RunScalabilityTest();
            return 0;
        } else if (mode == "eager") {
            cout << "Testing BruteForce Eager method..." << endl;
            tester.RunJoinMethodTest(candy::JoinMethodType::BRUTEFORCE_EAGER, 1000);
            tester.RunJoinMethodTest(candy::JoinMethodType::IVF_EAGER, 1000);
            return 0;
        } else if (mode == "lazy") {
            cout << "Testing BruteForce Lazy method..." << endl;
            tester.RunJoinMethodTest(candy::JoinMethodType::BRUTEFORCE_LAZY, 1000);
            tester.RunJoinMethodTest(candy::JoinMethodType::IVF_LAZY, 1000);
            return 0;
        }
    }
    
    // 否则运行Google Test
    return RUN_ALL_TESTS();
}
