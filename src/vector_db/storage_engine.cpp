#include <vector_db/storage_engine.h>

#include <fstream>
#include <sstream>
#include <utility>

namespace candy {

StorageEngine::StorageEngine(std::string storagePath) : storagePath(std::move(storagePath)) { load(); }

void StorageEngine::add(const std::shared_ptr<VectorRecord> &record) {
  data[record->id_] = record;
  writeToDisk(record->id_, *record->data_);
}

void StorageEngine::remove(const std::string &id) {
  data.erase(id);
  deleteFromDisk(id);
}

void StorageEngine::load() {
  std::ifstream in_file(storagePath + "/vectors.dat");
  std::string line;
  while (std::getline(in_file, line)) {
    std::istringstream iss(line);
    std::string id;
    iss >> id;

    VectorData vec;
    float value;
    while (iss >> value) {
      vec.push_back(value);
    }

    data[id] = std::make_shared<VectorRecord>(id, vec, 0);
  }
  in_file.close();
}

void StorageEngine::persist() {
  std::ofstream out_file(storagePath + "/vectors.dat");
  for (const auto &[id, record] : data) {
    out_file << id;
    for (const auto &val : *record->data_) {
      out_file << " " << val;
    }
    out_file << "\n";
  }
  out_file.close();
}

void StorageEngine::writeToDisk(const std::string &id, const VectorData &vec) {
  std::ofstream out_file(storagePath + "/vectors.dat", std::ios::app);
  out_file << id;
  for (const auto &val : vec) {
    out_file << " " << val;
  }
  out_file << "\n";
  out_file.close();
}

void StorageEngine::deleteFromDisk(const std::string &id) {
  // Placeholder: Actual implementation would rebuild the file or mark the entry
  // as deleted.
}

}  // namespace candy
