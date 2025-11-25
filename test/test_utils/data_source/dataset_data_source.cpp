#include "test_utils/data_source/dataset_data_source.h"
#include "utils/logger.h"
#include <algorithm>
#include <cctype>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace {

inline std::string trim_copy(const std::string& s) {
  size_t start = 0;
  while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) ++start;
  size_t end = s.size();
  while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) --end;
  return s.substr(start, end - start);
}

inline std::string parse_json_value(const std::string& line) {
  auto pos = line.find(':');
  if (pos == std::string::npos) return "";
  std::string value = trim_copy(line.substr(pos + 1));
  if (!value.empty() && value.back() == ',') {
    value.pop_back();
    value = trim_copy(value);
  }
  return value;
}

inline std::string strip_quotes(std::string value) {
  value = trim_copy(value);
  if (!value.empty() && value.front() == '"') {
    value.erase(value.begin());
  }
  if (!value.empty() && value.back() == '"') {
    value.pop_back();
  }
  return value;
}

}

namespace sageFlow { namespace test {

DatasetDataSource::DatasetDataSource(const Config& config)
    : config_(config), dimension_(0), current_index_(0) {
  loadVectors();
}

void DatasetDataSource::loadVectors() {
  namespace fs = std::filesystem;
  auto extension = fs::path(config_.file_path).extension().string();
  std::transform(extension.begin(), extension.end(), extension.begin(), [](unsigned char c){ return static_cast<char>(std::tolower(c)); });

  metadata_embedded_ = false;
  metadata_file_path_.clear();

  if (extension == ".json") {
    metadata_embedded_ = true;
    metadata_file_path_ = config_.file_path;
    loadVectorsFromJson();
  } else {
    metadata_file_path_ = config_.file_path + ".gt.json";
    loadVectorsFromFvecs();
    loadExternalGroundTruthIfAvailable();
  }
}

void DatasetDataSource::loadVectorsFromFvecs() {
  std::ifstream input(config_.file_path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("Cannot open file: " + config_.file_path);
  }

  while (true) {
    int32_t current_dim = 0;
    input.read(reinterpret_cast<char*>(&current_dim), sizeof(int32_t));

    if (input.eof()) {
      break;
    }
    if (input.fail()) {
      throw std::runtime_error("Error reading dimension from file: " + config_.file_path);
    }

    if (vectors_.empty()) {
      dimension_ = current_dim;
      if (config_.expected_dim != -1 && dimension_ != config_.expected_dim) {
        throw std::runtime_error("Unexpected dimension in file " + config_.file_path +
                                ". Expected " + std::to_string(config_.expected_dim) +
                                ", got " + std::to_string(dimension_));
      }
      if (dimension_ <= 0) {
        throw std::runtime_error("Invalid dimension read from file: " + std::to_string(dimension_));
      }
    } else if (current_dim != dimension_) {
      throw std::runtime_error("Inconsistent dimension found in file " + config_.file_path +
                              ". Expected " + std::to_string(dimension_) +
                              ", found " + std::to_string(current_dim) +
                              " at vector index " + std::to_string(vectors_.size()));
    }

    std::vector<float> vec(dimension_);
    input.read(reinterpret_cast<char*>(vec.data()), dimension_ * sizeof(float));
    if (input.fail()) {
      throw std::runtime_error("Error reading vector data from file: " + config_.file_path +
                              " at vector index " + std::to_string(vectors_.size()));
    }

    vectors_.push_back(std::move(vec));
  }

  input.close();

  if (vectors_.empty()) {
    throw std::runtime_error("No vectors loaded from file: " + config_.file_path);
  }

  SAGEFLOW_LOG_INFO("TEST", "[DatasetDataSource] Loaded {} vectors of dimension {} from {}",
                    vectors_.size(), dimension_, config_.file_path);
}

void DatasetDataSource::loadVectorsFromJson() {
  std::ifstream input(config_.file_path);
  if (!input.is_open()) {
    throw std::runtime_error("Cannot open file: " + config_.file_path);
  }

  vectors_.clear();
  ground_truth_entries_.clear();

  std::string line;
  bool in_vectors = false;
  bool in_ground_truth = false;
  bool in_gt_entry = false;
  bool in_pair_block = false;
  int declared_dimension = -1;
  int declared_count = -1;
  GroundTruthEntry current_entry;

  while (std::getline(input, line)) {
    std::string trimmed = trim_copy(line);
    if (trimmed.empty()) {
      continue;
    }

    if (!in_vectors && !in_ground_truth) {
      if (trimmed.rfind("\"dimension\"", 0) == 0) {
        auto value = parse_json_value(trimmed);
        if (!value.empty()) {
          declared_dimension = std::stoi(value);
        }
        continue;
      }
      if (trimmed.rfind("\"count\"", 0) == 0) {
        auto value = parse_json_value(trimmed);
        if (!value.empty()) {
          declared_count = std::stoi(value);
        }
        continue;
      }
      if (trimmed.find("\"vectors\"") != std::string::npos) {
        in_vectors = true;
        continue;
      }
      if (trimmed.find("\"ground_truth_sets\"") != std::string::npos) {
        in_ground_truth = true;
        continue;
      }
    }

    if (in_vectors) {
      if (trimmed == "[" || trimmed == "]" || trimmed == "],") {
        if (trimmed == "]" || trimmed == "],") {
          in_vectors = false;
        }
        continue;
      }

      bool has_comma = false;
      if (!trimmed.empty() && trimmed.back() == ',') {
        has_comma = true;
        trimmed.pop_back();
        trimmed = trim_copy(trimmed);
      }
      if (!trimmed.empty() && trimmed.front() == '[') {
        trimmed.erase(trimmed.begin());
      }
      if (!trimmed.empty() && trimmed.back() == ']') {
        trimmed.pop_back();
      }

      std::vector<float> vec;
      std::stringstream ss(trim_copy(trimmed));
      std::string token;
      while (std::getline(ss, token, ',')) {
        token = trim_copy(token);
        if (!token.empty()) {
          vec.push_back(std::stof(token));
        }
      }

      if (declared_dimension == -1) {
        declared_dimension = static_cast<int>(vec.size());
      }

      if (static_cast<int>(vec.size()) != declared_dimension) {
        throw std::runtime_error("Vector dimension mismatch in JSON file: " + config_.file_path);
      }

      vectors_.push_back(std::move(vec));
      (void)has_comma;
      continue;
    }

    if (in_ground_truth) {
      if (trimmed == "[" || trimmed == "]" || trimmed == "],") {
        if (trimmed == "]" || trimmed == "],") {
          in_ground_truth = false;
        }
        continue;
      }

      if (trimmed == "{") {
        current_entry = GroundTruthEntry{};
        in_gt_entry = true;
        continue;
      }
      if ((trimmed == "}," || trimmed == "}") && in_gt_entry && !in_pair_block) {
        ground_truth_entries_.push_back(std::move(current_entry));
        in_gt_entry = false;
        if (trimmed == "}") {
          in_ground_truth = false;
        }
        continue;
      }

      if (in_pair_block) {
        if (trimmed == "[") {
          continue;
        }
        if (trimmed == "[]" || trimmed == "[],") {
          in_pair_block = false;
          continue;
        }
        if (trimmed == "]" || trimmed == "],") {
          in_pair_block = false;
          continue;
        }

        bool has_comma = false;
        if (!trimmed.empty() && trimmed.back() == ',') {
          has_comma = true;
          trimmed.pop_back();
          trimmed = trim_copy(trimmed);
        }
        if (!trimmed.empty() && trimmed.front() == '[') {
          trimmed.erase(trimmed.begin());
        }
        if (!trimmed.empty() && trimmed.back() == ']') {
          trimmed.pop_back();
        }
        std::stringstream ss(trim_copy(trimmed));
        std::string token;
        std::vector<uint64_t> values;
        while (std::getline(ss, token, ',')) {
          token = trim_copy(token);
          if (!token.empty()) {
            values.push_back(static_cast<uint64_t>(std::stoull(token)));
          }
        }
        if (values.size() == 2) {
          current_entry.pairs.emplace_back(values[0], values[1]);
        }
        (void)has_comma;
        continue;
      }

      if (trimmed.rfind("\"label\"", 0) == 0) {
        current_entry.label = strip_quotes(parse_json_value(trimmed));
        continue;
      }
      if (trimmed.rfind("\"window_ms\"", 0) == 0) {
        current_entry.window_ms = static_cast<uint64_t>(std::stoull(parse_json_value(trimmed)));
        continue;
      }
      if (trimmed.rfind("\"similarity_threshold\"", 0) == 0) {
        current_entry.similarity_threshold = std::stod(parse_json_value(trimmed));
        continue;
      }
      if (trimmed.rfind("\"alpha\"", 0) == 0) {
        current_entry.alpha = std::stod(parse_json_value(trimmed));
        continue;
      }
      if (trimmed.rfind("\"modulo_base\"", 0) == 0) {
        current_entry.modulo_base = static_cast<uint64_t>(std::stoull(parse_json_value(trimmed)));
        continue;
      }
      if (trimmed.rfind("\"record_count\"", 0) == 0) {
        current_entry.record_count = static_cast<size_t>(std::stoull(parse_json_value(trimmed)));
        continue;
      }
      if (trimmed.rfind("\"pairs\"", 0) == 0) {
        in_pair_block = true;
        current_entry.pairs.clear();
        continue;
      }
    }
  }

  input.close();

  if (vectors_.empty()) {
    throw std::runtime_error("No vectors loaded from JSON file: " + config_.file_path);
  }

  dimension_ = declared_dimension;
  if (config_.expected_dim != -1 && dimension_ != config_.expected_dim) {
    throw std::runtime_error("Unexpected dimension in file " + config_.file_path +
                            ". Expected " + std::to_string(config_.expected_dim) +
                            ", got " + std::to_string(dimension_));
  }

  if (declared_count != -1 && declared_count != static_cast<int>(vectors_.size())) {
    SAGEFLOW_LOG_WARN("TEST", "[DatasetDataSource] Declared count {} differs from loaded {} in {}",
                      declared_count, vectors_.size(), config_.file_path);
  }

  SAGEFLOW_LOG_INFO("TEST", "[DatasetDataSource] Loaded {} vectors of dimension {} from JSON {} (GT entries: {})",
                    vectors_.size(), dimension_, config_.file_path, ground_truth_entries_.size());
}

std::vector<float> DatasetDataSource::getNextVector() {
  if (!hasMore()) {
    return std::vector<float>();
  }

  std::vector<float> result = vectors_[current_index_];
  current_index_++;

  // If looping is enabled and we reached the end, reset
  if (config_.loop && current_index_ >= vectors_.size()) {
    current_index_ = 0;
  }

  return result;
}

bool DatasetDataSource::hasMore() const {
  if (config_.loop) {
    return !vectors_.empty();  // Always has more if looping
  }
  return current_index_ < vectors_.size();
}

void DatasetDataSource::reset() {
  current_index_ = 0;
}

void DatasetDataSource::loadExternalGroundTruthIfAvailable() {
  if (metadata_embedded_ || metadata_file_path_.empty()) {
    return;
  }
  namespace fs = std::filesystem;
  if (!fs::exists(metadata_file_path_)) {
    SAGEFLOW_LOG_INFO("TEST", "[DatasetDataSource] No ground truth metadata found for {}", metadata_file_path_);
    return;
  }
  std::ifstream meta(metadata_file_path_);
  if (!meta.is_open()) {
    SAGEFLOW_LOG_WARN("TEST", "[DatasetDataSource] Unable to open metadata file {}", metadata_file_path_);
    return;
  }
  try {
    ground_truth_entries_.clear();
    loadGroundTruthFromJsonStream(meta);
    SAGEFLOW_LOG_INFO("TEST", "[DatasetDataSource] Loaded {} cached ground truth entries from {}",
                      ground_truth_entries_.size(), metadata_file_path_);
  } catch (const std::exception& e) {
    SAGEFLOW_LOG_WARN("TEST", "[DatasetDataSource] Failed to parse metadata {}: {}",
                      metadata_file_path_, e.what());
  }
}

void DatasetDataSource::loadGroundTruthFromJsonStream(std::istream& input) {
  std::string line;
  bool in_ground_truth = false;
  bool in_gt_entry = false;
  bool in_pair_block = false;
  GroundTruthEntry current_entry;

  while (std::getline(input, line)) {
    std::string trimmed = trim_copy(line);
    if (trimmed.empty()) continue;

    if (!in_ground_truth) {
      if (trimmed.find("\"ground_truth_sets\"") != std::string::npos) {
        in_ground_truth = true;
      }
      continue;
    }

    if (trimmed == "[" || trimmed == "]" || trimmed == "],") {
      if (trimmed == "]" || trimmed == "],") {
        in_ground_truth = false;
      }
      continue;
    }

    if (trimmed == "{") {
      current_entry = GroundTruthEntry{};
      in_gt_entry = true;
      continue;
    }
    if ((trimmed == "}," || trimmed == "}") && in_gt_entry && !in_pair_block) {
      ground_truth_entries_.push_back(std::move(current_entry));
      in_gt_entry = false;
      if (trimmed == "}") {
        in_ground_truth = false;
      }
      continue;
    }

    if (!in_gt_entry) {
      continue;
    }

    if (in_pair_block) {
      if (trimmed == "[") {
        continue;
      }
      if (trimmed == "[]" || trimmed == "[],") {
        in_pair_block = false;
        continue;
      }
      if (trimmed == "]" || trimmed == "],") {
        in_pair_block = false;
        continue;
      }
      if (!trimmed.empty() && trimmed.back() == ',') {
        trimmed.pop_back();
        trimmed = trim_copy(trimmed);
      }
      if (!trimmed.empty() && trimmed.front() == '[') {
        trimmed.erase(trimmed.begin());
      }
      if (!trimmed.empty() && trimmed.back() == ']') {
        trimmed.pop_back();
      }
      std::stringstream ss(trim_copy(trimmed));
      std::string token;
      std::vector<uint64_t> values;
      while (std::getline(ss, token, ',')) {
        token = trim_copy(token);
        if (!token.empty()) {
          values.push_back(static_cast<uint64_t>(std::stoull(token)));
        }
      }
      if (values.size() == 2) {
        current_entry.pairs.emplace_back(values[0], values[1]);
      }
      continue;
    }

    if (trimmed.rfind("\"label\"", 0) == 0) {
      current_entry.label = strip_quotes(parse_json_value(trimmed));
      continue;
    }
    if (trimmed.rfind("\"window_ms\"", 0) == 0) {
      current_entry.window_ms = static_cast<uint64_t>(std::stoull(parse_json_value(trimmed)));
      continue;
    }
    if (trimmed.rfind("\"similarity_threshold\"", 0) == 0) {
      current_entry.similarity_threshold = std::stod(parse_json_value(trimmed));
      continue;
    }
    if (trimmed.rfind("\"alpha\"", 0) == 0) {
      current_entry.alpha = std::stod(parse_json_value(trimmed));
      continue;
    }
    if (trimmed.rfind("\"modulo_base\"", 0) == 0) {
      current_entry.modulo_base = static_cast<uint64_t>(std::stoull(parse_json_value(trimmed)));
      continue;
    }
    if (trimmed.rfind("\"record_count\"", 0) == 0) {
      current_entry.record_count = static_cast<size_t>(std::stoull(parse_json_value(trimmed)));
      continue;
    }
    if (trimmed.rfind("\"pairs\"", 0) == 0) {
      in_pair_block = true;
      current_entry.pairs.clear();
      continue;
    }
  }
}

bool DatasetDataSource::writeGroundTruthMetadataFile(const std::string& path,
                                                     const std::vector<GroundTruthEntry>& entries) const {
  if (path.empty()) return false;
  namespace fs = std::filesystem;
  auto parent = fs::path(path).parent_path();
  if (!parent.empty()) {
    fs::create_directories(parent);
  }
  std::ofstream ofs(path);
  if (!ofs.is_open()) {
    SAGEFLOW_LOG_WARN("TEST", "[DatasetDataSource] Unable to open metadata output {}", path);
    return false;
  }
  ofs << std::fixed;
  ofs << "{\n  \"ground_truth_sets\": [\n";
  for (size_t idx = 0; idx < entries.size(); ++idx) {
    const auto& entry = entries[idx];
    ofs << "    {\n";
    if (!entry.label.empty()) {
      ofs << "      \"label\": \"" << entry.label << "\",\n";
    }
    ofs << "      \"window_ms\": " << entry.window_ms << ",\n";
    ofs << "      \"similarity_threshold\": " << entry.similarity_threshold << ",\n";
    ofs << "      \"alpha\": " << entry.alpha << ",\n";
    ofs << "      \"modulo_base\": " << entry.modulo_base << ",\n";
    ofs << "      \"record_count\": " << entry.record_count << ",\n";
    ofs << "      \"pair_count\": " << entry.pairs.size() << ",\n";
    ofs << "      \"pairs\": [\n";
    for (size_t p = 0; p < entry.pairs.size(); ++p) {
      ofs << "        [" << entry.pairs[p].first << ", " << entry.pairs[p].second << "]";
      if (p + 1 < entry.pairs.size()) ofs << ",";
      ofs << "\n";
    }
    ofs << "      ]\n";
    ofs << "    }";
    if (idx + 1 < entries.size()) {
      ofs << ",";
    }
    ofs << "\n";
  }
  ofs << "  ]\n}\n";
  ofs.close();
  return true;
}

std::optional<DatasetDataSource::GroundTruthEntry> DatasetDataSource::findGroundTruthEntry(
    uint64_t window_ms,
    double similarity_threshold,
    uint64_t modulo_base,
    size_t record_count) const {
  for (const auto& entry : ground_truth_entries_) {
    if (entry.window_ms == window_ms &&
        std::abs(entry.similarity_threshold - similarity_threshold) < 1e-9 &&
        entry.modulo_base == modulo_base &&
        (entry.record_count == 0 || entry.record_count == record_count)) {
      return entry;
    }
  }
  return std::nullopt;
}

bool DatasetDataSource::persistGroundTruthEntry(const GroundTruthEntry& entry) {
  namespace fs = std::filesystem;
  auto extension = fs::path(config_.file_path).extension().string();
  std::transform(extension.begin(), extension.end(), extension.begin(), [](unsigned char c){ return static_cast<char>(std::tolower(c)); });

  auto updated_entries = ground_truth_entries_;
  updated_entries.erase(std::remove_if(updated_entries.begin(), updated_entries.end(),
    [&](const GroundTruthEntry& gt){
      return gt.window_ms == entry.window_ms &&
             std::abs(gt.similarity_threshold - entry.similarity_threshold) < 1e-9 &&
             gt.modulo_base == entry.modulo_base &&
             (gt.record_count == entry.record_count || gt.record_count == 0 || entry.record_count == 0);
    }), updated_entries.end());
  updated_entries.push_back(entry);

  bool ok = false;
  if (extension == ".json") {
    JsonWriter writer;
    writer.setGroundTruthEntries(updated_entries);
    ok = writer.writeVectors(config_.file_path, vectors_, dimension_);
  } else if (extension == ".fvecs") {
    if (metadata_file_path_.empty()) {
      metadata_file_path_ = config_.file_path + ".gt.json";
    }
    ok = writeGroundTruthMetadataFile(metadata_file_path_, updated_entries);
  } else {
    SAGEFLOW_LOG_WARN("TEST", "[DatasetDataSource] Ground truth persistence unsupported for format {}", extension);
    ok = false;
  }

  if (ok) {
    ground_truth_entries_ = std::move(updated_entries);
  }
  return ok;
}

}} // namespace sageFlow::test
