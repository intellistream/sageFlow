#include <vector_db/storage_engine.h>

#include <cstdint>
#include <fstream>
#include <sstream>
#include <utility>

namespace candy {

StorageEngine::StorageEngine(std::string storage_path) : storage_path_(std::move(storage_path)) { load(); }

void StorageEngine::add(const std::shared_ptr<VectorRecord> &record) {
  data_[record->uid_] = record;
  writeToDisk(record->uid_, *record->data_);
}

void StorageEngine::remove(const uint64_t &id) {
  data_.erase(id);
  deleteFromDisk(id);
}

void StorageEngine::load() {
  std::ifstream in_file(storage_path_ + "/vectors.dat");
  std::string line;
  int dim = 0;
  std::getline(in_file, line);
  std::istringstream iss(line);
  iss >> dim;
  while (std::getline(in_file, line)) {
    std::istringstream iss(line);
    uint64_t id;
    iss >> id;
    VectorData vec(dim);
    float value;
    while (iss >> value) {
      vec.push_back(value);
    }

    data_[id] = std::make_shared<VectorRecord>(id, vec, 0);
  }
  in_file.close();
}

void StorageEngine::persist() {
  std::ofstream out_file(storage_path_ + "/vectors.dat");
  for (const auto &[id, record] : data_) {
    out_file << id;
    for (const auto &val : record->data_->data_) {
      out_file << " " << val;
    }
    out_file << "\n";
  }
  out_file.close();
}

void StorageEngine::writeToDisk(const uint64_t &id, const VectorData &vec) {
  std::ofstream out_file(storage_path_ + "/vectors.dat", std::ios::app);
  out_file << id;
  for (const auto &val : vec.data_) {
    out_file << " " << val;
  }
  out_file << "\n";
  out_file.close();
}

void StorageEngine::deleteFromDisk(const uint64_t &id) {
  // Placeholder: Actual implementation would rebuild the file or mark the entry
  // as deleted.
}

}  // namespace candy
