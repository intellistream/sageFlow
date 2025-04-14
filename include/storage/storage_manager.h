#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/data_types.h"
#include "compute_engine/compute_engine.h"

namespace candy {

class StorageManager {
 public:
  // data
  std::map<uint64_t, std::unique_ptr<VectorRecord>> data_;
  std ::shared_ptr<ComputeEngine> compute_engine_;

  // Constructor
  StorageManager() = default;

  // Destructor
  virtual ~StorageManager() = default;

  /**
   * * @brief Insert a record into the index.
   * * @param record The record to be inserted.
   * * @return true if the insertion was successful, false otherwise.
   */
  virtual auto insert(std::unique_ptr<VectorRecord> &record) -> bool = 0;

  /**
   * * @brief Insert a record into the index with a specific ID.
   * * @param record The record to be inserted.
   * * @param uid_t The ID of the record to be inserted.
   * * @return true if the insertion was successful, false otherwise.
   */
  virtual auto insert(std::unique_ptr<VectorData> &record, uint64_t uid) -> bool = 0;

  /**
   * * @brief Insert multiple records into the index.
   * * @param records The vector of records to be inserted.
   * * @return true if the insertion was successful, false otherwise.
   */
  virtual auto insert(std::vector<std::unique_ptr<VectorRecord>> &records) -> bool = 0;

  /**
   * * @brief erase a record from the index.
   * * @param vectorId The ID of the record to be erased.
   * * @return true if the erasure was successful, false otherwise.
   */
  virtual auto erase(uint64_t vector_id) -> bool = 0;

  /**
   * * @brief Erase multiple records from the index.
   * * @param vectorIds The vector of IDs of the records to be erased.
   * * @return true if the erasure was successful, false otherwise.
   */
  virtual auto erase(std::vector<uint64_t> &vector_ids) -> bool = 0;

  /**
   * * @brief Search for the top K nearest neighbors of a given record.
   * * @param record The record to search for.
   * * @param k The number of nearest neighbors to find.
   * * @return A vector of unique pointers to the nearest neighbor records.
   */
  virtual auto getVectorById(uint64_t vector_id) -> std::unique_ptr<VectorRecord> = 0;

  /**
   * * @brief Compute the distance between two records.
   * * @param record1 The first record.
   * * @param record2 The second record.
   * * @return The computed distance between the two records.
   */
  virtual auto distanceCompute(std::unique_ptr<VectorRecord> &record1,
                               std::unique_ptr<VectorRecord> &record2) -> float = 0;

  /**
   * * @brief Compute the distance between two records by their IDs.
   * * @param vectorId1 The ID of the first record.
   * * @param vectorId2 The ID of the second record.
   * * @return The computed distance between the two records.
   */
  virtual auto distanceCompute(uint64_t vector_id1, uint64_t vector_id2) -> float = 0;

  /**
   * * @brief Build the index.
   * * @return true if the indexing was successful, false otherwise.
   */
  virtual auto build() -> bool = 0;

  /**
   * * @brief Save the index to a file.
   * * @param path The path to the file where the index will be saved.
   * * @return true if the saving was successful, false otherwise.
   */

  virtual auto save(const std::string &path) -> bool = 0;
  /**
   * * @brief Load the index from a file.
   * * @param path The path to the file from which the index will be loaded.
   * * @return true if the loading was successful, false otherwise.
   */

  virtual auto load(const std::string &path) -> bool = 0;
  /**
   * * @brief Get the number of records in the index.
   * * @return The number of records in the index.
   */
  virtual auto getSize() -> size_t = 0;
};
}  // namespace candy