#pragma once

#include <vector>
#include <string>
#include <memory>

namespace sageFlow { namespace test {

/**
 * @brief Base class for writing vector data to files
 * 
 * Provides a unified interface for persisting vector data to different formats.
 * Implementations can support binary formats (fvecs), text formats (JSON, CSV), etc.
 */
class DataWriterBase {
public:
  virtual ~DataWriterBase() = default;

  /**
   * @brief Write vectors to a file
   * @param file_path Path to the output file
   * @param vectors Vector data to write (each inner vector is one data point)
   * @param dimension Vector dimension (for validation)
   * @return true if write was successful, false otherwise
   */
  virtual bool writeVectors(const std::string& file_path, 
                           const std::vector<std::vector<float>>& vectors,
                           int dimension) = 0;

  /**
   * @brief Get the file extension for this writer (e.g., ".fvecs", ".json")
   */
  virtual std::string getFileExtension() const = 0;

  /**
   * @brief Get a human-readable description of the format
   */
  virtual std::string getFormatDescription() const = 0;
};

}} // namespace sageFlow::test
