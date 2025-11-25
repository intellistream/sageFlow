#pragma once

#include "test_utils/data_source/data_source_base.h"
#include "test_utils/data_writer/json_writer.h"
#include <optional>
#include <string>
#include <istream>

namespace sageFlow { namespace test {

/**
 * @brief Data source that reads vectors from fvecs dataset files
 * 
 * Reads vector data from standard fvecs format files (commonly used in vector search benchmarks).
 * The fvecs format stores vectors as: [dimension(int)][vector_data(floats)]...
 */
class DatasetDataSource : public DataSourceBase {
public:
  struct Config {
    std::string file_path;
    bool loop = false;  // If true, loop back to start when reaching end
    int expected_dim = -1;  // Expected dimension, -1 means auto-detect
  };

  using GroundTruthEntry = JsonGroundTruthEntry;

  explicit DatasetDataSource(const Config& config);

  std::vector<float> getNextVector() override;
  int getDimension() const override { return dimension_; }
  bool hasMore() const override;
  void reset() override;
  int getTotalCount() const override { return static_cast<int>(vectors_.size()); }
  const std::string& getFilePath() const { return config_.file_path; }

  const std::vector<std::vector<float>>& getAllVectors() const { return vectors_; }

  const std::vector<GroundTruthEntry>& getGroundTruthEntries() const { return ground_truth_entries_; }
  std::optional<GroundTruthEntry> findGroundTruthEntry(uint64_t window_ms,
                                                      double similarity_threshold,
                                                      uint64_t modulo_base,
                                                      size_t record_count) const;
  bool persistGroundTruthEntry(const GroundTruthEntry& entry);
  const std::string& getMetadataFilePath() const { return metadata_file_path_; }

private:
  void loadVectors();
  void loadVectorsFromFvecs();
  void loadVectorsFromJson();
  void loadExternalGroundTruthIfAvailable();
  void loadGroundTruthFromJsonStream(std::istream& input);
  bool writeGroundTruthMetadataFile(const std::string& path,
                                    const std::vector<GroundTruthEntry>& entries) const;

  Config config_;
  std::vector<std::vector<float>> vectors_;
  int dimension_;
  size_t current_index_;
  std::string metadata_file_path_;
  bool metadata_embedded_ = false;
  std::vector<GroundTruthEntry> ground_truth_entries_;
};

}} // namespace sageFlow::test
