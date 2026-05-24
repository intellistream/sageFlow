#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>
#include <pybind11/functional.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

// C++ headers from sageFlow
#include "common/data_types.h"
#include "function/filter_function.h"
#include "function/map_function.h"
#include "function/join_function.h"
#include "function/window_function.h"
#include "function/aggregate_function.h"
#include "function/topk_function.h"
#include "function/itopk_function.h"
#include "function/sink_function.h"
#include "stream/stream.h"
#include "stream/stream_environment.h"
#include "stream/data_stream_source/simple_stream_source.h"
#include "stream/data_stream_source/streaming_source.h"

namespace py = pybind11;
using namespace sageFlow;  // NOLINT

inline VectorData createVectorDataFromNumpy(py::array_t<float> arr);

struct PersistentJoinPair {
    uint64_t left_uid;
    uint64_t right_uid;
    int64_t timestamp;
    double similarity;
};

inline double cosineSimilarity(const VectorRecord& left, const VectorRecord& right) {
    if (left.data_.dim_ != right.data_.dim_) {
        throw std::runtime_error("Cannot compare records with different dimensions");
    }

    const float* left_data = reinterpret_cast<const float*>(left.data_.data_.get());
    const float* right_data = reinterpret_cast<const float*>(right.data_.data_.get());
    double dot = 0.0;
    double left_norm = 0.0;
    double right_norm = 0.0;
    for (int32_t i = 0; i < left.data_.dim_; ++i) {
        dot += static_cast<double>(left_data[i]) * static_cast<double>(right_data[i]);
        left_norm += static_cast<double>(left_data[i]) * static_cast<double>(left_data[i]);
        right_norm += static_cast<double>(right_data[i]) * static_cast<double>(right_data[i]);
    }
    if (left_norm == 0.0 || right_norm == 0.0) {
        return 0.0;
    }
    return dot / (std::sqrt(left_norm) * std::sqrt(right_norm));
}

class PersistentVectorJoinRuntime {
 public:
    PersistentVectorJoinRuntime(
        int dim,
        std::string join_method,
        double similarity_threshold,
        int64_t window_size_ms,
        size_t queue_capacity,
        size_t parallelism)
        : dim_(dim),
          join_method_(std::move(join_method)),
          similarity_threshold_(similarity_threshold),
          window_size_ms_(window_size_ms),
          queue_capacity_(queue_capacity),
          parallelism_(parallelism == 0 ? 1 : parallelism) {
        if (dim_ <= 0) {
            throw std::runtime_error("dim must be positive");
        }
        if (similarity_threshold_ <= 0.0 || similarity_threshold_ > 1.0) {
            throw std::runtime_error("similarity_threshold must be in (0, 1]");
        }
        if (window_size_ms_ <= 0) {
            throw std::runtime_error("window_size_ms must be positive");
        }
    }

    ~PersistentVectorJoinRuntime() {
        close();
    }

    void start() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (running_) {
            return;
        }

        env_ = std::make_shared<StreamEnvironment>();
        left_source_ = std::make_shared<StreamingSource>("persistent_join_left", queue_capacity_);
        right_source_ = std::make_shared<StreamingSource>("persistent_join_right", queue_capacity_);

        auto join_func = std::make_unique<JoinFunction>(
            "persistent_join",
            [this](std::unique_ptr<VectorRecord>& left,
                   std::unique_ptr<VectorRecord>& right) -> std::unique_ptr<VectorRecord> {
                if (!left || !right || left->uid_ == right->uid_) {
                    return nullptr;
                }

                const auto ts = std::max(left->timestamp_, right->timestamp_);
                const double similarity = cosineSimilarity(*left, *right);
                {
                    std::lock_guard<std::mutex> pair_lock(mutex_);
                    emitted_pairs_.push_back(PersistentJoinPair{
                        left->uid_,
                        right->uid_,
                        ts,
                        similarity,
                    });
                }
                pair_cv_.notify_all();

                return std::make_unique<VectorRecord>(
                    left->uid_,
                    ts,
                    VectorData(left->data_.dim_, left->data_.type_, left->data_.data_.get()));
            },
            window_size_ms_,
            dim_);

        auto joined = left_source_->join(
            right_source_,
            std::move(join_func),
            join_method_,
            similarity_threshold_,
            parallelism_);
        joined->writeSink(
            std::make_unique<SinkFunction>(
                "persistent_join_sink",
                [](std::unique_ptr<VectorRecord>&) {}),
            1);

        env_->addStream(left_source_);
        env_->addStream(right_source_);
        env_->execute();
        running_ = true;
    }

    void addLeft(uint64_t uid, int64_t timestamp, py::array_t<float> arr) {
        addRecord(true, uid, timestamp, arr);
    }

    void addRight(uint64_t uid, int64_t timestamp, py::array_t<float> arr) {
        addRecord(false, uid, timestamp, arr);
    }

    size_t emittedPairCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return emitted_pairs_.size();
    }

    std::vector<PersistentJoinPair> pairsSince(size_t cursor) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (cursor >= emitted_pairs_.size()) {
            return {};
        }
        return std::vector<PersistentJoinPair>(emitted_pairs_.begin() + static_cast<std::ptrdiff_t>(cursor),
                                               emitted_pairs_.end());
    }

    bool waitForPairCount(size_t target_count, int timeout_ms) const {
        std::unique_lock<std::mutex> lock(mutex_);
        return pair_cv_.wait_for(
            lock,
            std::chrono::milliseconds(std::max(timeout_ms, 0)),
            [this, target_count] { return emitted_pairs_.size() >= target_count || !running_; });
    }

    py::dict runtimeInfo() const {
        std::lock_guard<std::mutex> lock(mutex_);
        py::dict info;
        info["mode"] = running_ ? "persistent_streaming_join" : "closed";
        info["join_method"] = join_method_;
        info["similarity_threshold"] = similarity_threshold_;
        info["window_size_ms"] = window_size_ms_;
        info["parallelism"] = parallelism_;
        info["retained_left_records"] = 0;
        info["retained_right_records"] = 0;
        info["queued_left_records"] = left_source_ ? left_source_->size() : 0;
        info["queued_right_records"] = right_source_ ? right_source_->size() : 0;
        info["emitted_pairs"] = emitted_pairs_.size();
        return info;
    }

    void reset() {
        close();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            emitted_pairs_.clear();
        }
        start();
    }

    void close() {
        std::shared_ptr<StreamEnvironment> env;
        std::shared_ptr<StreamingSource> left;
        std::shared_ptr<StreamingSource> right;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!running_ && !env_) {
                return;
            }
            env = env_;
            left = left_source_;
            right = right_source_;
            running_ = false;
            env_.reset();
            left_source_.reset();
            right_source_.reset();
        }

        if (left) {
            left->finish();
        }
        if (right) {
            right->finish();
        }
        if (env) {
            try {
                env->stop();
                env->awaitTermination();
            } catch (...) {
                // Destructors must not throw across the Python boundary.
            }
        }
        pair_cv_.notify_all();
    }

 private:
    void addRecord(bool left_side, uint64_t uid, int64_t timestamp, py::array_t<float> arr) {
        if (arr.request().ndim != 1 || arr.request().shape[0] != dim_) {
            throw std::runtime_error("record vector dimension does not match runtime dim");
        }
        start();

        auto data = createVectorDataFromNumpy(arr);
        std::shared_ptr<StreamingSource> source;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            source = left_side ? left_source_ : right_source_;
        }
        if (!source) {
            throw std::runtime_error("persistent runtime source is not available");
        }

        py::gil_scoped_release release;
        if (!source->addRecord(uid, timestamp, std::move(data))) {
            throw std::runtime_error("persistent runtime rejected record because the source is closed");
        }
    }

    int dim_;
    std::string join_method_;
    double similarity_threshold_;
    int64_t window_size_ms_;
    size_t queue_capacity_;
    size_t parallelism_;

    mutable std::mutex mutex_;
    mutable std::condition_variable pair_cv_;
    std::shared_ptr<StreamEnvironment> env_;
    std::shared_ptr<StreamingSource> left_source_;
    std::shared_ptr<StreamingSource> right_source_;
    std::vector<PersistentJoinPair> emitted_pairs_;
    bool running_ = false;
};

// Helper function to create VectorData from numpy array
inline VectorData createVectorDataFromNumpy(py::array_t<float> arr) {
    auto buf = arr.request();
    if (buf.ndim != 1) {
        throw std::runtime_error("Array must be 1D");
    }
    int32_t dim = static_cast<int32_t>(buf.shape[0]);
    auto bytes = static_cast<size_t>(dim) * sizeof(float);
    auto *data = new char[bytes];
    std::memcpy(data, buf.ptr, bytes);
    return VectorData(dim, DataType::Float32, data);
}

// Helper function to extract numpy array from VectorRecord
inline py::array_t<float> extractNumpyFromRecord(const VectorRecord& rec) {
    const float* data_ptr = reinterpret_cast<const float*>(rec.data_.data_.get());
    int32_t dim = rec.data_.dim_;
    py::array_t<float> result(dim);
    auto buf = result.request();
    std::memcpy(buf.ptr, data_ptr, static_cast<size_t>(dim) * sizeof(float));
    return result;
}

PYBIND11_MODULE(_sage_flow, m) {
    m.doc() = "SageFlow - Vector-native stream processing engine for LLM inference pipelines";

    // ==================== Enums ====================
    
    py::enum_<DataType>(m, "DataType", py::module_local())
        .value("None", DataType::None)
        .value("Int8", DataType::Int8)
        .value("Int16", DataType::Int16)
        .value("Int32", DataType::Int32)
        .value("Int64", DataType::Int64)
        .value("Float32", DataType::Float32)
        .value("Float64", DataType::Float64)
        .export_values();

    py::enum_<FunctionType>(m, "FunctionType", py::module_local())
        .value("None", FunctionType::None)
        .value("Filter", FunctionType::Filter)
        .value("Map", FunctionType::Map)
        .value("Join", FunctionType::Join)
        .value("Sink", FunctionType::Sink)
        .value("Topk", FunctionType::Topk)
        .value("Window", FunctionType::Window)
        .value("ITopk", FunctionType::ITopk)
        .value("Aggregate", FunctionType::Aggregate)
        .export_values();

    py::enum_<WindowType>(m, "WindowType", py::module_local())
        .value("Sliding", WindowType::Sliding)
        .value("Tumbling", WindowType::Tumbling)
        .export_values();

    py::enum_<AggregateType>(m, "AggregateType", py::module_local())
        .value("None", AggregateType::None)
        .value("Avg", AggregateType::Avg)
        .export_values();

    // ==================== Data Types ====================

    py::class_<VectorData>(m, "VectorData", py::module_local())
        .def(py::init<int32_t, DataType>(), py::arg("dim"), py::arg("dtype"))
        .def(py::init([](int32_t dim, DataType type, py::array_t<float> arr) {
            auto buf = arr.request();
            if (buf.ndim != 1 || buf.shape[0] != dim) {
                throw std::runtime_error("Array shape mismatch");
            }
            auto bytes = static_cast<size_t>(dim) * sizeof(float);
            auto *data = new char[bytes];
            std::memcpy(data, buf.ptr, bytes);
            return VectorData(dim, type, data);
        }), py::arg("dim"), py::arg("dtype"), py::arg("data"))
        .def(py::init([](py::array_t<float> arr) {
            return createVectorDataFromNumpy(arr);
        }), py::arg("data"))
        .def_readonly("dim", &VectorData::dim_)
        .def_readonly("dtype", &VectorData::type_)
        .def("to_numpy", [](const VectorData& self) {
            const float* data_ptr = reinterpret_cast<const float*>(self.data_.get());
            py::array_t<float> result(self.dim_);
            auto buf = result.request();
            std::memcpy(buf.ptr, data_ptr, static_cast<size_t>(self.dim_) * sizeof(float));
            return result;
        });

    py::class_<VectorRecord>(m, "VectorRecord", py::module_local())
        .def(py::init<const uint64_t&, const int64_t&, const VectorData&>(),
             py::arg("uid"), py::arg("timestamp"), py::arg("data"))
        .def(py::init([](uint64_t uid, int64_t ts, py::array_t<float> arr) {
            return VectorRecord(uid, ts, createVectorDataFromNumpy(arr));
        }), py::arg("uid"), py::arg("timestamp"), py::arg("data"))
        .def_readonly("uid", &VectorRecord::uid_)
        .def_readonly("timestamp", &VectorRecord::timestamp_)
        .def_readonly("data", &VectorRecord::data_)
        .def("to_numpy", [](const VectorRecord& self) {
            return extractNumpyFromRecord(self);
        });

    py::class_<PersistentJoinPair>(m, "PersistentJoinPair", py::module_local())
        .def_readonly("left_uid", &PersistentJoinPair::left_uid)
        .def_readonly("right_uid", &PersistentJoinPair::right_uid)
        .def_readonly("timestamp", &PersistentJoinPair::timestamp)
        .def_readonly("similarity", &PersistentJoinPair::similarity)
        .def("to_dict", [](const PersistentJoinPair& self) {
            py::dict payload;
            payload["left_uid"] = self.left_uid;
            payload["right_uid"] = self.right_uid;
            payload["timestamp"] = self.timestamp;
            payload["similarity"] = self.similarity;
            return payload;
        });

    py::class_<PersistentVectorJoinRuntime>(m, "PersistentVectorJoinRuntime", py::module_local(),
        "Long-lived two-input SageFlow join runtime backed by StreamingSource and StreamEnvironment.")
        .def(py::init<int, std::string, double, int64_t, size_t, size_t>(),
             py::arg("dim"),
             py::arg("join_method") = "bruteforce_lazy",
             py::arg("similarity_threshold") = 0.985,
             py::arg("window_size_ms") = 24 * 60 * 60 * 1000,
             py::arg("queue_capacity") = 1024,
             py::arg("parallelism") = 1)
        .def("start", &PersistentVectorJoinRuntime::start,
             "Start the persistent StreamingSource-backed join graph.")
        .def("add_left", &PersistentVectorJoinRuntime::addLeft,
             py::arg("uid"), py::arg("timestamp"), py::arg("data"),
             "Append a record to the left side of the persistent join.")
        .def("add_right", &PersistentVectorJoinRuntime::addRight,
             py::arg("uid"), py::arg("timestamp"), py::arg("data"),
             "Append a record to the right side of the persistent join.")
        .def("emitted_pair_count", &PersistentVectorJoinRuntime::emittedPairCount)
        .def("pairs_since", &PersistentVectorJoinRuntime::pairsSince, py::arg("cursor"))
        .def("wait_for_pair_count", &PersistentVectorJoinRuntime::waitForPairCount,
             py::arg("target_count"), py::arg("timeout_ms") = 100)
        .def("runtime_info", &PersistentVectorJoinRuntime::runtimeInfo)
        .def("reset", &PersistentVectorJoinRuntime::reset)
        .def("close", &PersistentVectorJoinRuntime::close);

    // ==================== Function Classes ====================

    // Base Function class (abstract)
    py::class_<Function, std::shared_ptr<Function>>(m, "Function", py::module_local())
        .def("getName", &Function::getName)
        .def("getType", &Function::getType);

    // FilterFunction with Python callback support
    py::class_<FilterFunction, Function, std::shared_ptr<FilterFunction>>(m, "FilterFunction", py::module_local())
        .def(py::init<std::string>(), py::arg("name"))
        .def(py::init([](const std::string& name, py::function filter_cb) {
            auto cpp_func = [filter_cb](std::unique_ptr<VectorRecord>& rec) -> bool {
                py::gil_scoped_acquire gil;
                try {
                    py::object result = filter_cb(rec->uid_, rec->timestamp_, extractNumpyFromRecord(*rec));
                    return result.cast<bool>();
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python filter callback error: ") + e.what());
                }
            };
            return std::make_shared<FilterFunction>(name, cpp_func);
        }), py::arg("name"), py::arg("filter_func"),
        "Create FilterFunction with Python callback: filter_func(uid, timestamp, data_numpy) -> bool")
        .def("setFilterFunc", [](FilterFunction& self, py::function filter_cb) {
            auto cpp_func = [filter_cb](std::unique_ptr<VectorRecord>& rec) -> bool {
                py::gil_scoped_acquire gil;
                try {
                    py::object result = filter_cb(rec->uid_, rec->timestamp_, extractNumpyFromRecord(*rec));
                    return result.cast<bool>();
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python filter callback error: ") + e.what());
                }
            };
            self.setFilterFunc(cpp_func);
        }, py::arg("filter_func"));

    // MapFunction with Python callback support
    py::class_<MapFunction, Function, std::shared_ptr<MapFunction>>(m, "MapFunction", py::module_local())
        .def(py::init<std::string>(), py::arg("name"))
        .def(py::init([](const std::string& name, py::function map_cb) {
            auto cpp_func = [map_cb](std::unique_ptr<VectorRecord>& rec) -> void {
                py::gil_scoped_acquire gil;
                try {
                    py::object result = map_cb(rec->uid_, rec->timestamp_, extractNumpyFromRecord(*rec));
                    // If callback returns a numpy array, update the record's data in-place
                    if (!result.is_none() && py::isinstance<py::array_t<float>>(result)) {
                        py::array_t<float> new_data = result.cast<py::array_t<float>>();
                        auto buf = new_data.request();
                        if (buf.ndim == 1) {
                            int32_t new_dim = static_cast<int32_t>(buf.shape[0]);
                            // Only update if dimensions match (in-place update)
                            if (new_dim == rec->data_.dim_) {
                                std::memcpy(rec->data_.data_.get(), buf.ptr, 
                                           static_cast<size_t>(new_dim) * sizeof(float));
                            } else {
                                // Dimension changed - need to create new record
                                auto bytes = static_cast<size_t>(new_dim) * sizeof(float);
                                auto* new_bytes = new char[bytes];
                                std::memcpy(new_bytes, buf.ptr, bytes);
                                rec = std::make_unique<VectorRecord>(rec->uid_, rec->timestamp_, 
                                    VectorData(new_dim, DataType::Float32, new_bytes));
                            }
                        }
                    }
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python map callback error: ") + e.what());
                }
            };
            return std::make_shared<MapFunction>(name, cpp_func);
        }), py::arg("name"), py::arg("map_func"),
        "Create MapFunction with Python callback: map_func(uid, timestamp, data_numpy) -> Optional[numpy.ndarray]")
        .def("setMapFunc", [](MapFunction& self, py::function map_cb) {
            auto cpp_func = [map_cb](std::unique_ptr<VectorRecord>& rec) -> void {
                py::gil_scoped_acquire gil;
                try {
                    py::object result = map_cb(rec->uid_, rec->timestamp_, extractNumpyFromRecord(*rec));
                    if (!result.is_none() && py::isinstance<py::array_t<float>>(result)) {
                        py::array_t<float> new_data = result.cast<py::array_t<float>>();
                        auto buf = new_data.request();
                        if (buf.ndim == 1) {
                            int32_t new_dim = static_cast<int32_t>(buf.shape[0]);
                            if (new_dim == rec->data_.dim_) {
                                std::memcpy(rec->data_.data_.get(), buf.ptr, 
                                           static_cast<size_t>(new_dim) * sizeof(float));
                            } else {
                                auto bytes = static_cast<size_t>(new_dim) * sizeof(float);
                                auto* new_bytes = new char[bytes];
                                std::memcpy(new_bytes, buf.ptr, bytes);
                                rec = std::make_unique<VectorRecord>(rec->uid_, rec->timestamp_, 
                                    VectorData(new_dim, DataType::Float32, new_bytes));
                            }
                        }
                    }
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python map callback error: ") + e.what());
                }
            };
            self.setMapFunc(cpp_func);
        }, py::arg("map_func"));

    // JoinFunction with Python callback support
    py::class_<JoinFunction, Function, std::shared_ptr<JoinFunction>>(m, "JoinFunction", py::module_local())
        .def(py::init<std::string, int>(), py::arg("name"), py::arg("dim"))
        .def(py::init([](const std::string& name, py::function join_cb, int dim) {
            auto cpp_func = [join_cb](std::unique_ptr<VectorRecord>& left, std::unique_ptr<VectorRecord>& right) 
                -> std::unique_ptr<VectorRecord> {
                py::gil_scoped_acquire gil;
                try {
                    py::object result = join_cb(
                        left->uid_, left->timestamp_, extractNumpyFromRecord(*left),
                        right->uid_, right->timestamp_, extractNumpyFromRecord(*right)
                    );
                    if (result.is_none()) {
                        return nullptr;
                    }
                    // Expect tuple (uid, timestamp, data_numpy) or VectorRecord
                    if (py::isinstance<py::tuple>(result)) {
                        py::tuple t = result.cast<py::tuple>();
                        if (t.size() != 3) {
                            throw std::runtime_error("Join callback must return (uid, timestamp, data) tuple or None");
                        }
                        uint64_t uid = t[0].cast<uint64_t>();
                        int64_t ts = t[1].cast<int64_t>();
                        py::array_t<float> arr = t[2].cast<py::array_t<float>>();
                        return std::make_unique<VectorRecord>(uid, ts, createVectorDataFromNumpy(arr));
                    }
                    throw std::runtime_error("Join callback must return (uid, timestamp, data) tuple or None");
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python join callback error: ") + e.what());
                }
            };
            return std::make_shared<JoinFunction>(name, cpp_func, dim);
        }), py::arg("name"), py::arg("join_func"), py::arg("dim"),
        "Create JoinFunction: join_func(left_uid, left_ts, left_data, right_uid, right_ts, right_data) -> (uid, ts, data) or None")
        .def(py::init([](const std::string& name, py::function join_cb, int64_t time_window, int dim) {
            auto cpp_func = [join_cb](std::unique_ptr<VectorRecord>& left, std::unique_ptr<VectorRecord>& right) 
                -> std::unique_ptr<VectorRecord> {
                py::gil_scoped_acquire gil;
                try {
                    py::object result = join_cb(
                        left->uid_, left->timestamp_, extractNumpyFromRecord(*left),
                        right->uid_, right->timestamp_, extractNumpyFromRecord(*right)
                    );
                    if (result.is_none()) {
                        return nullptr;
                    }
                    if (py::isinstance<py::tuple>(result)) {
                        py::tuple t = result.cast<py::tuple>();
                        if (t.size() != 3) {
                            throw std::runtime_error("Join callback must return (uid, timestamp, data) tuple or None");
                        }
                        uint64_t uid = t[0].cast<uint64_t>();
                        int64_t ts = t[1].cast<int64_t>();
                        py::array_t<float> arr = t[2].cast<py::array_t<float>>();
                        return std::make_unique<VectorRecord>(uid, ts, createVectorDataFromNumpy(arr));
                    }
                    throw std::runtime_error("Join callback must return (uid, timestamp, data) tuple or None");
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python join callback error: ") + e.what());
                }
            };
            return std::make_shared<JoinFunction>(name, cpp_func, time_window, dim);
        }), py::arg("name"), py::arg("join_func"), py::arg("time_window"), py::arg("dim"))
        .def("getDim", &JoinFunction::getDim)
        .def("getWindowSize", &JoinFunction::getWindowSize)
        .def("getStepSize", &JoinFunction::getStepSize)
        .def("setWindow", &JoinFunction::setWindow, py::arg("time_window"), py::arg("step_size"));

    // WindowFunction
    py::class_<WindowFunction, Function, std::shared_ptr<WindowFunction>>(m, "WindowFunction", py::module_local())
        .def(py::init<std::string>(), py::arg("name"))
        .def(py::init<std::string, int, int, WindowType>(),
             py::arg("name"), py::arg("window_size"), py::arg("slide_size"), py::arg("window_type"))
        .def("getWindowType", &WindowFunction::getWindowType)
        .def("getWindowSize", &WindowFunction::getWindowSize)
        .def("getSlideSize", &WindowFunction::getSlideSize);

    // AggregateFunction
    py::class_<AggregateFunction, Function, std::shared_ptr<AggregateFunction>>(m, "AggregateFunction", py::module_local())
        .def(py::init<const std::string&>(), py::arg("name"))
        .def(py::init<const std::string&, AggregateType>(), py::arg("name"), py::arg("aggregate_type"))
        .def("getAggregateType", &AggregateFunction::getAggregateType);

    // TopkFunction
    py::class_<TopkFunction, Function, std::shared_ptr<TopkFunction>>(m, "TopkFunction", py::module_local())
        .def(py::init<const std::string&>(), py::arg("name"))
        .def(py::init<const std::string&, int, int>(), py::arg("name"), py::arg("k"), py::arg("index_id"))
        .def("getK", &TopkFunction::getK)
        .def("getIndexId", &TopkFunction::getIndexId);

    // ITopkFunction
    py::class_<ITopkFunction, Function, std::shared_ptr<ITopkFunction>>(m, "ITopkFunction", py::module_local())
        .def(py::init<const std::string&>(), py::arg("name"))
        .def(py::init([](const std::string& name, int k, int dim, uint64_t uid, int64_t ts, py::array_t<float> arr) {
            auto record = std::make_unique<VectorRecord>(uid, ts, createVectorDataFromNumpy(arr));
            return std::make_shared<ITopkFunction>(name, k, dim, std::move(record));
        }), py::arg("name"), py::arg("k"), py::arg("dim"), py::arg("uid"), py::arg("timestamp"), py::arg("query_vector"))
        .def("getK", &ITopkFunction::getK)
        .def("getDim", &ITopkFunction::getDim);

    // SinkFunction with Python callback support
    py::class_<SinkFunction, Function, std::shared_ptr<SinkFunction>>(m, "SinkFunction", py::module_local())
        .def(py::init<std::string>(), py::arg("name"))
        .def(py::init([](const std::string& name, py::function sink_cb) {
            auto cpp_func = [sink_cb](std::unique_ptr<VectorRecord>& rec) -> void {
                py::gil_scoped_acquire gil;
                try {
                    sink_cb(rec->uid_, rec->timestamp_, extractNumpyFromRecord(*rec));
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python sink callback error: ") + e.what());
                }
            };
            return std::make_shared<SinkFunction>(name, cpp_func);
        }), py::arg("name"), py::arg("sink_func"),
        "Create SinkFunction with callback: sink_func(uid, timestamp, data_numpy)")
        .def("setSinkFunc", [](SinkFunction& self, py::function sink_cb) {
            auto cpp_func = [sink_cb](std::unique_ptr<VectorRecord>& rec) -> void {
                py::gil_scoped_acquire gil;
                try {
                    sink_cb(rec->uid_, rec->timestamp_, extractNumpyFromRecord(*rec));
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python sink callback error: ") + e.what());
                }
            };
            self.setSinkFunc(cpp_func);
        }, py::arg("sink_func"));

    // ==================== Stream Class ====================

    py::class_<Stream, std::shared_ptr<Stream>>(m, "Stream", py::module_local())
        .def(py::init<std::string>(), py::arg("name"))
        .def_readwrite("name", &Stream::name_)
        .def("getParallelism", &Stream::getParallelism)
        .def("setParallelism", &Stream::setParallelism, py::arg("parallelism"))
        
        // Filter operation with Python callback
        .def("filter", [](Stream& self, py::function filter_cb, size_t parallelism) {
            auto cpp_func = [filter_cb](std::unique_ptr<VectorRecord>& rec) -> bool {
                py::gil_scoped_acquire gil;
                try {
                    py::object result = filter_cb(rec->uid_, rec->timestamp_, extractNumpyFromRecord(*rec));
                    return result.cast<bool>();
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python filter callback error: ") + e.what());
                }
            };
            auto filter_fn = std::make_unique<FilterFunction>("py_filter", cpp_func);
            return self.filter(std::move(filter_fn), parallelism);
        }, py::arg("filter_func"), py::arg("parallelism") = 1,
        "Apply filter: filter_func(uid, timestamp, data_numpy) -> bool")
        
        // Map operation with Python callback
        .def("map", [](Stream& self, py::function map_cb, size_t parallelism) {
            auto cpp_func = [map_cb](std::unique_ptr<VectorRecord>& rec) -> void {
                py::gil_scoped_acquire gil;
                try {
                    py::object result = map_cb(rec->uid_, rec->timestamp_, extractNumpyFromRecord(*rec));
                    if (!result.is_none() && py::isinstance<py::array_t<float>>(result)) {
                        py::array_t<float> new_data = result.cast<py::array_t<float>>();
                        auto buf = new_data.request();
                        if (buf.ndim == 1) {
                            int32_t new_dim = static_cast<int32_t>(buf.shape[0]);
                            if (new_dim == rec->data_.dim_) {
                                std::memcpy(rec->data_.data_.get(), buf.ptr, 
                                           static_cast<size_t>(new_dim) * sizeof(float));
                            } else {
                                auto bytes = static_cast<size_t>(new_dim) * sizeof(float);
                                auto* new_bytes = new char[bytes];
                                std::memcpy(new_bytes, buf.ptr, bytes);
                                rec = std::make_unique<VectorRecord>(rec->uid_, rec->timestamp_, 
                                    VectorData(new_dim, DataType::Float32, new_bytes));
                            }
                        }
                    }
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python map callback error: ") + e.what());
                }
            };
            auto map_fn = std::make_unique<MapFunction>("py_map", cpp_func);
            return self.map(std::move(map_fn), parallelism);
        }, py::arg("map_func"), py::arg("parallelism") = 1,
        "Apply map: map_func(uid, timestamp, data_numpy) -> Optional[numpy.ndarray]")

        // Join operation with Python callback
        .def("join", [](Stream& self, std::shared_ptr<Stream> other_stream, py::function join_cb, 
                        int dim, size_t parallelism) {
            auto cpp_func = [join_cb](std::unique_ptr<VectorRecord>& left, std::unique_ptr<VectorRecord>& right) 
                -> std::unique_ptr<VectorRecord> {
                py::gil_scoped_acquire gil;
                try {
                    py::object result = join_cb(
                        left->uid_, left->timestamp_, extractNumpyFromRecord(*left),
                        right->uid_, right->timestamp_, extractNumpyFromRecord(*right)
                    );
                    if (result.is_none()) {
                        return nullptr;
                    }
                    if (py::isinstance<py::tuple>(result)) {
                        py::tuple t = result.cast<py::tuple>();
                        if (t.size() != 3) {
                            throw std::runtime_error("Join callback must return (uid, timestamp, data) tuple or None");
                        }
                        uint64_t uid = t[0].cast<uint64_t>();
                        int64_t ts = t[1].cast<int64_t>();
                        py::array_t<float> arr = t[2].cast<py::array_t<float>>();
                        return std::make_unique<VectorRecord>(uid, ts, createVectorDataFromNumpy(arr));
                    }
                    throw std::runtime_error("Join callback must return (uid, timestamp, data) tuple or None");
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python join callback error: ") + e.what());
                }
            };
            auto join_fn = std::make_unique<JoinFunction>("py_join", cpp_func, dim);
            return self.join(other_stream, std::move(join_fn), parallelism);
        }, py::arg("other_stream"), py::arg("join_func"), py::arg("dim"), py::arg("parallelism") = 1,
        "Join streams: join_func(l_uid, l_ts, l_data, r_uid, r_ts, r_data) -> (uid, ts, data) or None")

        // Join with method and threshold
        .def("join", [](Stream& self, std::shared_ptr<Stream> other_stream, py::function join_cb,
                        int dim, const std::string& join_method, double similarity_threshold, 
                        size_t parallelism) {
            auto cpp_func = [join_cb](std::unique_ptr<VectorRecord>& left, std::unique_ptr<VectorRecord>& right) 
                -> std::unique_ptr<VectorRecord> {
                py::gil_scoped_acquire gil;
                try {
                    py::object result = join_cb(
                        left->uid_, left->timestamp_, extractNumpyFromRecord(*left),
                        right->uid_, right->timestamp_, extractNumpyFromRecord(*right)
                    );
                    if (result.is_none()) {
                        return nullptr;
                    }
                    if (py::isinstance<py::tuple>(result)) {
                        py::tuple t = result.cast<py::tuple>();
                        if (t.size() != 3) {
                            throw std::runtime_error("Join callback must return (uid, timestamp, data) tuple or None");
                        }
                        uint64_t uid = t[0].cast<uint64_t>();
                        int64_t ts = t[1].cast<int64_t>();
                        py::array_t<float> arr = t[2].cast<py::array_t<float>>();
                        return std::make_unique<VectorRecord>(uid, ts, createVectorDataFromNumpy(arr));
                    }
                    throw std::runtime_error("Join callback must return (uid, timestamp, data) tuple or None");
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python join callback error: ") + e.what());
                }
            };
            auto join_fn = std::make_unique<JoinFunction>("py_join", cpp_func, dim);
            return self.join(other_stream, std::move(join_fn), join_method, similarity_threshold, parallelism);
        }, py::arg("other_stream"), py::arg("join_func"), py::arg("dim"),
           py::arg("join_method"), py::arg("similarity_threshold"), py::arg("parallelism") = 1,
        "Join with method config: join_method (e.g., 'bruteforce_lazy', 'ivf', 'hnsw')")

        // Join with method, threshold, and window_size_ms
        .def("join", [](Stream& self, std::shared_ptr<Stream> other_stream, py::function join_cb,
                        int dim, const std::string& join_method, double similarity_threshold,
                        int64_t window_size_ms, size_t parallelism) {
            auto cpp_func = [join_cb](std::unique_ptr<VectorRecord>& left, std::unique_ptr<VectorRecord>& right) 
                -> std::unique_ptr<VectorRecord> {
                py::gil_scoped_acquire gil;
                try {
                    py::object result = join_cb(
                        left->uid_, left->timestamp_, extractNumpyFromRecord(*left),
                        right->uid_, right->timestamp_, extractNumpyFromRecord(*right)
                    );
                    if (result.is_none()) {
                        return nullptr;
                    }
                    if (py::isinstance<py::tuple>(result)) {
                        py::tuple t = result.cast<py::tuple>();
                        if (t.size() != 3) {
                            throw std::runtime_error("Join callback must return (uid, timestamp, data) tuple or None");
                        }
                        uint64_t uid = t[0].cast<uint64_t>();
                        int64_t ts = t[1].cast<int64_t>();
                        py::array_t<float> arr = t[2].cast<py::array_t<float>>();
                        return std::make_unique<VectorRecord>(uid, ts, createVectorDataFromNumpy(arr));
                    }
                    throw std::runtime_error("Join callback must return (uid, timestamp, data) tuple or None");
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python join callback error: ") + e.what());
                }
            };
            auto join_fn = std::make_unique<JoinFunction>("py_join", cpp_func, dim);
            auto result_stream = self.join(other_stream, std::move(join_fn), join_method, similarity_threshold, parallelism);
            // Set window_size_ms via JoinStrategyConfig
            JoinStrategyConfig config;
            config.window_size_ms = window_size_ms;
            config.similarity_threshold = similarity_threshold;
            config.dimension = dim;
            result_stream->setJoinStrategyConfig(config);
            return result_stream;
        }, py::arg("other_stream"), py::arg("join_func"), py::arg("dim"),
           py::arg("join_method"), py::arg("similarity_threshold"), 
           py::arg("window_size_ms"), py::arg("parallelism") = 1,
        "Join with method config and window size: window_size_ms controls the time window for join matching")

        // Window operation
        .def("window", [](Stream& self, int window_size, int slide_size, WindowType window_type, 
                          size_t parallelism) {
            auto window_fn = std::make_unique<WindowFunction>("py_window", window_size, slide_size, window_type);
            return self.window(std::move(window_fn), parallelism);
        }, py::arg("window_size"), py::arg("slide_size"), 
           py::arg("window_type") = WindowType::Sliding, py::arg("parallelism") = 1,
        "Apply window operation")

        // Aggregate operation
        .def("aggregate", [](Stream& self, AggregateType agg_type, size_t parallelism) {
            auto agg_fn = std::make_unique<AggregateFunction>("py_aggregate", agg_type);
            return self.aggregate(std::move(agg_fn), parallelism);
        }, py::arg("aggregate_type") = AggregateType::Avg, py::arg("parallelism") = 1,
        "Apply aggregate operation")

        // TopK operation
        .def("topk", &Stream::topk, py::arg("index_id"), py::arg("k"), py::arg("parallelism") = 1,
        "Apply TopK operation using index")

        // ITopK operation with query vector
        .def("itopk", [](Stream& self, int k, int dim, uint64_t uid, int64_t ts, 
                         py::array_t<float> query_vector, size_t parallelism) {
            auto record = std::make_unique<VectorRecord>(uid, ts, createVectorDataFromNumpy(query_vector));
            auto itopk_fn = std::make_unique<ITopkFunction>("py_itopk", k, dim, std::move(record));
            return self.itopk(std::move(itopk_fn), parallelism);
        }, py::arg("k"), py::arg("dim"), py::arg("uid"), py::arg("timestamp"), 
           py::arg("query_vector"), py::arg("parallelism") = 1,
        "Apply ITopK (incremental TopK) operation with query vector")

        // WriteSink with Python callback (full data)
        .def("writeSink", [](Stream& self, py::function sink_cb, size_t parallelism) {
            auto cpp_func = [sink_cb](std::unique_ptr<VectorRecord>& rec) -> void {
                py::gil_scoped_acquire gil;
                try {
                    sink_cb(rec->uid_, rec->timestamp_, extractNumpyFromRecord(*rec));
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python sink callback error: ") + e.what());
                }
            };
            auto sink_fn = std::make_unique<SinkFunction>("py_sink", cpp_func);
            return self.writeSink(std::move(sink_fn), parallelism);
        }, py::arg("sink_func"), py::arg("parallelism") = 1,
        "Write to sink: sink_func(uid, timestamp, data_numpy)")

        // Legacy API for backward compatibility
        .def("write_sink_py", [](Stream& self, const std::string& name, py::function cb) {
            auto fn = SinkFunction(name, [cb](std::unique_ptr<VectorRecord>& rec) {
                py::gil_scoped_acquire gil;
                cb(rec->uid_, rec->timestamp_);
            });
            auto fn_ptr = std::make_unique<SinkFunction>(std::move(fn));
            return self.writeSink(std::move(fn_ptr));
        }, py::arg("name"), py::arg("callback"),
        "Legacy sink API: callback(uid, timestamp) - use writeSink for full data access")

        // Join configuration
        .def("setJoinMethod", &Stream::setJoinMethod, py::arg("method"))
        .def("setJoinSimilarityThreshold", &Stream::setJoinSimilarityThreshold, py::arg("threshold"))
        .def("getJoinMethod", &Stream::getJoinMethod)
        .def("getJoinSimilarityThreshold", &Stream::getJoinSimilarityThreshold);

    // ==================== SimpleStreamSource ====================

    py::class_<SimpleStreamSource, std::shared_ptr<SimpleStreamSource>, Stream>(m, "SimpleStreamSource", py::module_local())
        .def(py::init<std::string>(), py::arg("name"))
        .def("addRecord", py::overload_cast<const VectorRecord&>(&SimpleStreamSource::addRecord), py::arg("record"))
        .def("addRecord", [](SimpleStreamSource& self, uint64_t uid, int64_t ts, py::array_t<float> arr) {
            self.addRecord(uid, ts, createVectorDataFromNumpy(arr));
        }, py::arg("uid"), py::arg("timestamp"), py::arg("data"),
        "Add record with numpy array data")
        
        // Inherit all Stream methods for chaining
        .def("filter", [](SimpleStreamSource& self, py::function filter_cb, size_t parallelism) {
            auto cpp_func = [filter_cb](std::unique_ptr<VectorRecord>& rec) -> bool {
                py::gil_scoped_acquire gil;
                try {
                    py::object result = filter_cb(rec->uid_, rec->timestamp_, extractNumpyFromRecord(*rec));
                    return result.cast<bool>();
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python filter callback error: ") + e.what());
                }
            };
            auto filter_fn = std::make_unique<FilterFunction>("py_filter", cpp_func);
            return self.filter(std::move(filter_fn), parallelism);
        }, py::arg("filter_func"), py::arg("parallelism") = 1)

        .def("map", [](SimpleStreamSource& self, py::function map_cb, size_t parallelism) {
            auto cpp_func = [map_cb](std::unique_ptr<VectorRecord>& rec) -> void {
                py::gil_scoped_acquire gil;
                try {
                    py::object result = map_cb(rec->uid_, rec->timestamp_, extractNumpyFromRecord(*rec));
                    if (!result.is_none() && py::isinstance<py::array_t<float>>(result)) {
                        py::array_t<float> new_data = result.cast<py::array_t<float>>();
                        auto buf = new_data.request();
                        if (buf.ndim == 1) {
                            int32_t new_dim = static_cast<int32_t>(buf.shape[0]);
                            if (new_dim == rec->data_.dim_) {
                                std::memcpy(rec->data_.data_.get(), buf.ptr, 
                                           static_cast<size_t>(new_dim) * sizeof(float));
                            } else {
                                auto bytes = static_cast<size_t>(new_dim) * sizeof(float);
                                auto* new_bytes = new char[bytes];
                                std::memcpy(new_bytes, buf.ptr, bytes);
                                rec = std::make_unique<VectorRecord>(rec->uid_, rec->timestamp_, 
                                    VectorData(new_dim, DataType::Float32, new_bytes));
                            }
                        }
                    }
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python map callback error: ") + e.what());
                }
            };
            auto map_fn = std::make_unique<MapFunction>("py_map", cpp_func);
            return self.map(std::move(map_fn), parallelism);
        }, py::arg("map_func"), py::arg("parallelism") = 1)

        // Note: join() methods are inherited from Stream base class
        // SimpleStreamSource inherits both join() overloads from Stream:
        //   - join(other, join_func, dim, parallelism=1) - basic version
        //   - join(other, join_func, dim, join_method, similarity_threshold, parallelism=1) - with config

        .def("window", [](SimpleStreamSource& self, int window_size, int slide_size, 
                          WindowType window_type, size_t parallelism) {
            auto window_fn = std::make_unique<WindowFunction>("py_window", window_size, slide_size, window_type);
            return self.window(std::move(window_fn), parallelism);
        }, py::arg("window_size"), py::arg("slide_size"), 
           py::arg("window_type") = WindowType::Sliding, py::arg("parallelism") = 1)

        .def("aggregate", [](SimpleStreamSource& self, AggregateType agg_type, size_t parallelism) {
            auto agg_fn = std::make_unique<AggregateFunction>("py_aggregate", agg_type);
            return self.aggregate(std::move(agg_fn), parallelism);
        }, py::arg("aggregate_type") = AggregateType::Avg, py::arg("parallelism") = 1)

        .def("topk", &SimpleStreamSource::topk, py::arg("index_id"), py::arg("k"), py::arg("parallelism") = 1)

        .def("writeSink", [](SimpleStreamSource& self, py::function sink_cb, size_t parallelism) {
            auto cpp_func = [sink_cb](std::unique_ptr<VectorRecord>& rec) -> void {
                py::gil_scoped_acquire gil;
                try {
                    sink_cb(rec->uid_, rec->timestamp_, extractNumpyFromRecord(*rec));
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python sink callback error: ") + e.what());
                }
            };
            auto sink_fn = std::make_unique<SinkFunction>("py_sink", cpp_func);
            return self.writeSink(std::move(sink_fn), parallelism);
        }, py::arg("sink_func"), py::arg("parallelism") = 1)

        .def("write_sink_py", [](SimpleStreamSource& self, const std::string& name, py::function cb) {
            auto fn = SinkFunction(name, [cb](std::unique_ptr<VectorRecord>& rec) {
                py::gil_scoped_acquire gil;
                cb(rec->uid_, rec->timestamp_);
            });
            auto fn_ptr = std::make_unique<SinkFunction>(std::move(fn));
            return self.writeSink(std::move(fn_ptr));
        }, py::arg("name"), py::arg("callback"));

    // ==================== StreamingSource ====================
    // StreamingSource 支持动态流式输入：先启动 pipeline，再动态添加数据
    
    py::class_<StreamingSource, std::shared_ptr<StreamingSource>, Stream>(m, "StreamingSource", py::module_local(),
        R"doc(
        StreamingSource - 支持动态流式输入的数据源
        
        与 SimpleStreamSource 不同，StreamingSource 支持：
        1. 先创建数据源和 pipeline，调用 execute() 启动
        2. 然后动态添加记录（线程安全）
        3. 最后调用 finish() 标记流结束
        
        Example:
            >>> import sage_flow as sf
            >>> import numpy as np
            >>> 
            >>> env = sf.StreamEnvironment()
            >>> source = sf.StreamingSource("my_stream", capacity=1000)
            >>> 
            >>> # 构建 pipeline
            >>> source.filter(lambda uid, ts, data: np.linalg.norm(data) > 0.5)
            >>>        .writeSink(lambda uid, ts, data: print(f"Got {uid}"))
            >>> env.addStream(source)
            >>> 
            >>> # 启动（非阻塞）
            >>> env.execute()
            >>> 
            >>> # 动态添加数据
            >>> for i, vec in enumerate(vectors):
            >>>     source.addRecord(i, int(time.time() * 1000), vec)
            >>> 
            >>> # 标记结束并等待
            >>> source.finish()
            >>> env.awaitTermination()
        )doc")
        .def(py::init<std::string, size_t>(), 
             py::arg("name"), py::arg("capacity") = 10000,
             "Create StreamingSource with name and optional capacity (0=unlimited)")
        
        // 添加记录 - 阻塞版本
        .def("addRecord", py::overload_cast<const VectorRecord&>(&StreamingSource::addRecord), 
             py::arg("record"),
             "Add a record (blocks if queue is full)")
        .def("addRecord", [](StreamingSource& self, uint64_t uid, int64_t ts, py::array_t<float> arr) {
            // 在持有 GIL 时先复制 numpy 数据
            VectorData vec_data = createVectorDataFromNumpy(arr);
            // 然后释放 GIL 以允许其他 Python 线程运行（特别是消费者线程）
            py::gil_scoped_release release;
            return self.addRecord(uid, ts, std::move(vec_data));
        }, py::arg("uid"), py::arg("timestamp"), py::arg("data"),
        "Add record with numpy array (blocks if queue is full)")
        
        // 添加记录 - 非阻塞版本
        .def("tryAddRecord", py::overload_cast<const VectorRecord&>(&StreamingSource::tryAddRecord),
             py::arg("record"),
             "Try to add a record without blocking. Returns True if successful.")
        .def("tryAddRecord", [](StreamingSource& self, uint64_t uid, int64_t ts, py::array_t<float> arr) {
            return self.tryAddRecord(uid, ts, createVectorDataFromNumpy(arr));
        }, py::arg("uid"), py::arg("timestamp"), py::arg("data"),
        "Try to add record without blocking. Returns True if successful.")
        
        // 流控制
        .def("finish", &StreamingSource::finish,
             "Mark the stream as finished. No more records can be added after this.")
        .def("isFinished", &StreamingSource::isFinished,
             "Check if the stream has been marked as finished.")
        
        // 状态查询
        .def("size", &StreamingSource::size,
             "Get current number of records in the queue.")
        .def("capacity", &StreamingSource::capacity,
             "Get queue capacity (0 means unlimited).")
        .def("setCapacity", &StreamingSource::setCapacity, py::arg("capacity"),
             "Set queue capacity (0 means unlimited).")
        
        // 继承 Stream 的所有方法用于链式调用
        .def("filter", [](StreamingSource& self, py::function filter_cb, size_t parallelism) {
            auto cpp_func = [filter_cb](std::unique_ptr<VectorRecord>& rec) -> bool {
                py::gil_scoped_acquire gil;
                try {
                    py::object result = filter_cb(rec->uid_, rec->timestamp_, extractNumpyFromRecord(*rec));
                    return result.cast<bool>();
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python filter callback error: ") + e.what());
                }
            };
            auto filter_fn = std::make_unique<FilterFunction>("py_filter", cpp_func);
            return self.filter(std::move(filter_fn), parallelism);
        }, py::arg("filter_func"), py::arg("parallelism") = 1)

        .def("map", [](StreamingSource& self, py::function map_cb, size_t parallelism) {
            auto cpp_func = [map_cb](std::unique_ptr<VectorRecord>& rec) -> void {
                py::gil_scoped_acquire gil;
                try {
                    py::object result = map_cb(rec->uid_, rec->timestamp_, extractNumpyFromRecord(*rec));
                    if (!result.is_none() && py::isinstance<py::array_t<float>>(result)) {
                        py::array_t<float> new_data = result.cast<py::array_t<float>>();
                        auto buf = new_data.request();
                        if (buf.ndim == 1) {
                            int32_t new_dim = static_cast<int32_t>(buf.shape[0]);
                            if (new_dim == rec->data_.dim_) {
                                std::memcpy(rec->data_.data_.get(), buf.ptr, 
                                           static_cast<size_t>(new_dim) * sizeof(float));
                            } else {
                                auto bytes = static_cast<size_t>(new_dim) * sizeof(float);
                                auto* new_bytes = new char[bytes];
                                std::memcpy(new_bytes, buf.ptr, bytes);
                                rec = std::make_unique<VectorRecord>(rec->uid_, rec->timestamp_, 
                                    VectorData(new_dim, DataType::Float32, new_bytes));
                            }
                        }
                    }
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python map callback error: ") + e.what());
                }
            };
            auto map_fn = std::make_unique<MapFunction>("py_map", cpp_func);
            return self.map(std::move(map_fn), parallelism);
        }, py::arg("map_func"), py::arg("parallelism") = 1)

        .def("window", [](StreamingSource& self, int window_size, int slide_size, 
                          WindowType window_type, size_t parallelism) {
            auto window_fn = std::make_unique<WindowFunction>("py_window", window_size, slide_size, window_type);
            return self.window(std::move(window_fn), parallelism);
        }, py::arg("window_size"), py::arg("slide_size"), 
           py::arg("window_type") = WindowType::Sliding, py::arg("parallelism") = 1)

        .def("aggregate", [](StreamingSource& self, AggregateType agg_type, size_t parallelism) {
            auto agg_fn = std::make_unique<AggregateFunction>("py_aggregate", agg_type);
            return self.aggregate(std::move(agg_fn), parallelism);
        }, py::arg("aggregate_type") = AggregateType::Avg, py::arg("parallelism") = 1)

        .def("topk", &StreamingSource::topk, py::arg("index_id"), py::arg("k"), py::arg("parallelism") = 1)

        .def("writeSink", [](StreamingSource& self, py::function sink_cb, size_t parallelism) {
            auto cpp_func = [sink_cb](std::unique_ptr<VectorRecord>& rec) -> void {
                py::gil_scoped_acquire gil;
                try {
                    sink_cb(rec->uid_, rec->timestamp_, extractNumpyFromRecord(*rec));
                } catch (const py::error_already_set& e) {
                    throw std::runtime_error(std::string("Python sink callback error: ") + e.what());
                }
            };
            auto sink_fn = std::make_unique<SinkFunction>("py_sink", cpp_func);
            return self.writeSink(std::move(sink_fn), parallelism);
        }, py::arg("sink_func"), py::arg("parallelism") = 1);

    // ==================== StreamEnvironment ====================

    py::class_<StreamEnvironment>(m, "StreamEnvironment", py::module_local())
        .def(py::init<>())
        .def("addStream", &StreamEnvironment::addStream, py::arg("stream"),
        "Add a stream to the environment")
        .def("execute", &StreamEnvironment::execute,
        "Execute all registered streams (non-blocking for StreamingSource)")
        .def("stop", &StreamEnvironment::stop,
        "Stop execution")
        .def("awaitTermination", &StreamEnvironment::awaitTermination,
        "Wait for execution to complete");

    // ==================== Module-level convenience functions ====================

    m.def("create_source", [](const std::string& name) {
        return std::make_shared<SimpleStreamSource>(name);
    }, py::arg("name"), "Create a new SimpleStreamSource (for batch data)");

    m.def("create_streaming_source", [](const std::string& name, size_t capacity) {
        return std::make_shared<StreamingSource>(name, capacity);
    }, py::arg("name"), py::arg("capacity") = 10000, 
    "Create a new StreamingSource (for dynamic streaming data)");

    m.def("create_environment", []() {
        return StreamEnvironment();
    }, "Create a new StreamEnvironment");
}
