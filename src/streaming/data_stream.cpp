#include <streaming/data_stream.h>

#include <list>

namespace candy {

auto DataStream::filter(const FilterFunction &filterFunc) -> DataStream * {
  transformations_.emplace_back([this, filterFunc] {
    auto it = records_.begin();
    while (it != records_.end()) {
      if (filterFunc(*it)) {
        ++it;
      } else {
        it = records_.erase(it);
      }
    }
  });
  return this;
}

auto DataStream::map(const MapFunction &mapFunc) -> DataStream * {
  transformations_.emplace_back([this, mapFunc] {
    for (auto &record : records_) {
      auto new_record = mapFunc(record);
      std::swap(record, new_record);
    }
  });
  return this;
}

auto DataStream::join(const std::shared_ptr<DataStream> &otherStream, const JoinFunction &joinFunc) -> DataStream * {
  
  transformations_.emplace_back([this, otherStream, joinFunc] {
    std::list<std::shared_ptr<VectorRecord>> new_records;
    auto l_it = records_.begin();
    while (l_it != records_.end()) {
      const auto &left_record = *l_it;
      const auto &other_records = otherStream->records_;
      auto r_it = other_records.begin();
      while (r_it != other_records.end()) {
        const auto &right_record = *r_it;
        if (joinFunc(left_record, right_record)) {
          new_records.push_back(left_record);
        }
        ++r_it;
      }
      ++l_it;
    }
    records_ = new_records;
  });
  return this;
}

void DataStream::writeSink(const std::string &sinkName, const SinkFunction &sinkFunc) {
  transformations_.emplace_back([this, sinkFunc] {
    for (const auto &record : records_) {
      sinkFunc(record);
    }
  });
}

void DataStream::addRecord(const std::shared_ptr<VectorRecord> &record) { records_.push_back(record); }

void DataStream::processStream() { executeTransformations(); }

void DataStream::executeTransformations() {
  for (const auto &transformation : transformations_) {
    transformation();
  }
  transformations_.clear();
}

}  // namespace candy
