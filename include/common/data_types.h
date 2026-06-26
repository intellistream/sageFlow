#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>
#include <iostream>

namespace sageFlow {
enum DataType {  // NOLINT
  None,
  Int8,
  Int16,
  Int32,
  Int64,
  Float32,
  Float64,
};

constexpr int DATA_TYPE_SIZE[7] = {0, 1, 2, 4, 8, 4, 8};  // Size of each data type in bytes

struct VectorData {
  int32_t dim_;                   // Dimension of the vector
  DataType type_;                 // Data type of the vector
  std::unique_ptr<char[]> data_;  // Pointer to the vector data

  // Constructor to initialize the vector data
  VectorData(int32_t dim, DataType type, char *data);

  explicit VectorData(int32_t dim, DataType type);

  VectorData(const VectorData &other);  // Copy constructor

  auto operator==(const VectorData &other) const -> bool;
  // Equality operator

  auto operator!=(const VectorData &other) const -> bool;
  // Inequality operator

  void printData(std::ostream &os = std::cout) const;
  bool Serialize(std::ostream &out) const;
  bool Deserialize(std::istream &in);
};

// Wrapper for vector data with metadata (e.g., ID, timestamp)

struct VectorRecord {
  const uint64_t uid_;       // Unique identifier for the vector
  const int64_t timestamp_;  // Timestamp for the record
  VectorData data_;          // Shared pointer to the vector data

  /// Bitmask of subtask indices this record was routed to (multicast routing).
  /// Bit i set  ⇒  record lives in subtask i's WindowState.
  /// Used by Owner-Computes dedup: only the lowest-indexed subtask in the
  /// intersection of query_mask & candidate_mask emits a match.
  /// Supports up to 64 subtasks (uint64_t). 0 = unset / legacy path.
  uint64_t routing_mask_ = 0;

  // Constructor with move semantics for efficiency
  VectorRecord(const uint64_t &uid, const int64_t &timestamp, VectorData &&data);

  // Constructor with copy semantics
  VectorRecord(const uint64_t &uid, const int64_t &timestamp, const VectorData &data);

  // Constructor with a raw data pointer
  VectorRecord(const uint64_t &uid, const int64_t &timestamp, int32_t dim, DataType type, char *data);

  // Equality operator for comparisons
  auto operator==(const VectorRecord &other) const -> bool;
  void printRecord(std::ostream &os = std::cout) const;
  bool Serialize(std::ostream &out) const;
  bool Deserialize(std::istream &in);
};
// 算子内部状态层的统一记录视图：不可变共享所有权。
// 用于窗口状态 / StorageManager / 候选 / 快照之间以引用计数共享同一实例，
// 避免在热路径上重复深拷贝 VectorRecord（含 VectorData 的 char[]）。
// 注意：算子间传输层（Response/队列）仍使用 unique_ptr<VectorRecord> + move，不受影响。
using RecordView = std::shared_ptr<const VectorRecord>;

// 配对物化载荷（R1）：Join 命中后以只读共享引用携带左右两条原始记录 + 相似度，
// 不深拷贝向量体、不拼接新向量。供面向 LLM 前处理的下游算子零拷贝读取两条 payload。
struct RecordPairPayload {
  RecordView left;
  RecordView right;
  double similarity;

  RecordPairPayload(RecordView l, RecordView r, double sim)
      : left(std::move(l)), right(std::move(r)), similarity(sim) {}
};

// 扩展 ResponseType：增加 Exit 与 EOFMarker 用于流水线优雅关闭；RecordPair 用于配对物化。
// 新增值追加在末尾，保证既有 switch(type_) 分支与序号不变（加法兼容）。
enum class ResponseType { None, Record, List, Exit, EOFMarker, RecordPair };  // NOLINT

struct Response {
  ResponseType type_;
  std::unique_ptr<VectorRecord> record_;
  std::unique_ptr<std::vector<std::unique_ptr<VectorRecord>>> records_;
  std::unique_ptr<RecordPairPayload> pair_;  // 仅 RecordPair 使用，其余路径恒为 nullptr

  Response() : type_(ResponseType::None), record_(nullptr) {}

  Response(const ResponseType type, std::unique_ptr<VectorRecord> record) : type_(type), record_(std::move(record)) {}

  Response(const ResponseType type, std::unique_ptr<std::vector<std::unique_ptr<VectorRecord>>> records)
      : type_(type), records_(std::move(records)) {}

  Response(const ResponseType type, std::unique_ptr<RecordPairPayload> pair) : type_(type), pair_(std::move(pair)) {}

  Response(const Response &other) { copyFrom(other); }

  Response &operator=(const Response &other) {
    if (this != &other) {
      record_.reset();
      records_.reset();
      pair_.reset();
      copyFrom(other);
    }
    return *this;
  }

  // 显式移动语义：拷贝构造/赋值的存在会抑制隐式移动，导致入队 TaggedResponse 时
  // std::move(Response) 退化为深拷贝。这里恢复移动语义，使入队真正零拷贝。
  Response(Response &&) noexcept = default;
  Response &operator=(Response &&) noexcept = default;

 private:
  void copyFrom(const Response &other) {
    type_ = other.type_;
    if (other.record_) {
      record_ = std::make_unique<VectorRecord>(*other.record_);
    }
    if (other.records_) {
      records_ = std::make_unique<std::vector<std::unique_ptr<VectorRecord>>>();
      records_->reserve(other.records_->size());
      for (const auto &rec : *other.records_) {
        records_->emplace_back(std::make_unique<VectorRecord>(*rec));
      }
    }
    // RecordPair 载荷的拷贝是浅层共享：拷贝 RecordView(shared_ptr)，不复制底层只读记录。
    if (other.pair_) {
      pair_ = std::make_unique<RecordPairPayload>(*other.pair_);
    }
  }
};

struct UidAndDist {
  uint64_t uid_;
  double distance_;

  UidAndDist(uint64_t uid, double distance) : uid_(uid), distance_(distance) {}

  // 重载小于号，以构建一个按 distance 比较的最大堆
  auto operator<(const UidAndDist& other) const -> bool {
    return distance_ < other.distance_;
  }
};

}  // namespace sageFlow
