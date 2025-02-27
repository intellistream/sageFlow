// data_stream.h
#pragma once

#include <core/common/data_types.h>

#include <functional>
#include <list>
#include <memory>
#include <queue>
#include <string>
#include <utility>
#include <vector>

namespace candy {

class DataStream {
 public:
  using FilterFunction = std::function<bool(const std::shared_ptr<VectorRecord>&)>;
  using MapFunction = std::function<std::shared_ptr<VectorRecord>(const std::shared_ptr<VectorRecord>&)>;
  using JoinFunction = std::function<bool(const std::shared_ptr<VectorRecord>&, const std::shared_ptr<VectorRecord>&)>;
  using SinkFunction = std::function<void(const std::shared_ptr<VectorRecord>&)>;

  // Constructor
  explicit DataStream(std::string name) : name_(std::move(name)) {}

  // Apply a filter to the stream
  auto filter(const FilterFunction& filterFunc) -> DataStream*;

  // Apply a map function to the stream
  auto map(const MapFunction& mapFunc) -> DataStream*;

  // Join with another stream
  auto join(const std::shared_ptr<DataStream>& otherStream, const JoinFunction& joinFunc) -> DataStream*;

  // Write to a sink
  void writeSink(const std::string& sinkName, const SinkFunction& sinkFunc);

  // Internal: Add data to the stream
  void addRecord(const std::shared_ptr<VectorRecord>& record);

  // Internal: Process the stream
  void processStream();

 private:
  std::string name_;
  std::list<std::shared_ptr<VectorRecord>> records_;
  std::vector<std::function<void()>> transformations_;

  // Execute transformations
  void executeTransformations();
};

}  // namespace candy
