#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>
#include <pybind11/functional.h>

// C++ headers from sageFlow
#include "common/data_types.h"
#include "function/sink_function.h"
#include "stream/stream.h"
#include "stream/stream_environment.h"
#include "stream/data_stream_source/simple_stream_source.h"

namespace py = pybind11;
using namespace sageFlow;  // NOLINT

PYBIND11_MODULE(_sage_flow, m) {
    m.doc() = "SAGE Flow - Stream processing engine";

    // Enums - use module_local to avoid type conflicts with other extensions
    py::enum_<DataType>(m, "DataType", py::module_local())
        .value("None", DataType::None)
        .value("Int8", DataType::Int8)
        .value("Int16", DataType::Int16)
        .value("Int32", DataType::Int32)
        .value("Int64", DataType::Int64)
        .value("Float32", DataType::Float32)
        .value("Float64", DataType::Float64);

    // VectorData - use module_local to avoid conflicts
    py::class_<VectorData>(m, "VectorData", py::module_local())
        .def(py::init<int32_t, DataType>())
        .def(py::init([](int32_t dim, DataType type, py::array_t<float> arr) {
            auto buf = arr.request();
            if (buf.ndim != 1 || buf.shape[0] != dim) {
                throw std::runtime_error("Array shape mismatch");
            }
            auto bytes = static_cast<size_t>(dim) * sizeof(float);
            auto *data = new char[bytes];
            std::memcpy(data, buf.ptr, bytes);
            return VectorData(dim, type, data);
        }))
        .def(py::init([](py::array_t<float> arr) {
            auto buf = arr.request();
            if (buf.ndim != 1) {
                throw std::runtime_error("Array must be 1D");
            }
            int32_t dim = static_cast<int32_t>(buf.shape[0]);
            auto bytes = static_cast<size_t>(dim) * sizeof(float);
            auto *data = new char[bytes];
            std::memcpy(data, buf.ptr, bytes);
            return VectorData(dim, DataType::Float32, data);
        }));

    // VectorRecord - use module_local to avoid conflicts
    py::class_<VectorRecord>(m, "VectorRecord", py::module_local())
        .def(py::init<const uint64_t&, const int64_t&, const VectorData&>())
        .def_readonly("uid", &VectorRecord::uid_)
        .def_readonly("timestamp", &VectorRecord::timestamp_)
        .def_readonly("data", &VectorRecord::data_);

    // Stream - use module_local to avoid conflicts
    py::class_<Stream, std::shared_ptr<Stream>>(m, "Stream", py::module_local())
        .def(py::init<std::string>())
        // Minimal API: only bind a Python-friendly sink writer used by examples
        .def("write_sink_py", [](Stream &self, const std::string &name, py::function cb) {
            auto fn = SinkFunction(name, [cb](std::unique_ptr<VectorRecord> &rec) {
                py::gil_scoped_acquire gil;
                cb(rec->uid_, rec->timestamp_);
            });
            auto fn_ptr = std::make_unique<SinkFunction>(std::move(fn));
            return self.writeSink(std::move(fn_ptr));
        }, py::arg("name"), py::arg("callback"));

    // SimpleStreamSource - use module_local to avoid conflicts
    py::class_<SimpleStreamSource, std::shared_ptr<SimpleStreamSource>, Stream>(m, "SimpleStreamSource", py::module_local())
        .def(py::init<std::string>())
        .def("addRecord", py::overload_cast<const VectorRecord &>(&SimpleStreamSource::addRecord))
        .def("addRecord", [](SimpleStreamSource &self, uint64_t uid, int64_t ts, py::array_t<float> arr) {
            auto buf = arr.request();
            if (buf.ndim != 1) {
                throw std::runtime_error("Array must be 1D");
            }
            int32_t dim = static_cast<int32_t>(buf.shape[0]);
            auto bytes = static_cast<size_t>(dim) * sizeof(float);
            auto *data = new char[bytes];
            std::memcpy(data, buf.ptr, bytes);
            VectorData vec(dim, DataType::Float32, data);
            self.addRecord(uid, ts, std::move(vec));
        })
        .def("write_sink_py", [](SimpleStreamSource &self, const std::string &name, py::function cb) {
            auto fn = SinkFunction(name, [cb](std::unique_ptr<VectorRecord> &rec) {
                py::gil_scoped_acquire gil;
                cb(rec->uid_, rec->timestamp_);
            });
            auto fn_ptr = std::make_unique<SinkFunction>(std::move(fn));
            return self.writeSink(std::move(fn_ptr));
        }, py::arg("name"), py::arg("callback"));

    // StreamEnvironment - use module_local to avoid conflicts
    py::class_<StreamEnvironment>(m, "StreamEnvironment", py::module_local())
        .def(py::init<>())
        .def("addStream", &StreamEnvironment::addStream)
        .def("execute", &StreamEnvironment::execute);
}
