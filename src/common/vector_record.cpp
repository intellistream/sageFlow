#include "common/vector_record.h"
#include <cstring>

namespace candy {

// Implement serialization method for VectorRecord
auto VectorRecord::Serialize(std::ostream &out) const -> bool {
  // Write UID
  out.write(reinterpret_cast<const char*>(&uid_), sizeof(uid_));
  if (!out.good()) return false;
  
  // Write timestamp
  out.write(reinterpret_cast<const char*>(&timestamp_), sizeof(timestamp_));
  if (!out.good()) return false;
  
  // Write vector data
  return data_.Serialize(out);
}

// Implement deserialization method for VectorRecord
auto VectorRecord::Deserialize(std::istream &in) -> bool {
  // This is a bit tricky since VectorRecord members are const
  // We'll need to read into temporary variables first

  // Read UID and timestamp into temporary variables
  uint64_t uid;
  timestamp_t timestamp;
  
  in.read(reinterpret_cast<char*>(&uid), sizeof(uid));
  if (!in.good()) return false;
  
  in.read(reinterpret_cast<char*>(&timestamp), sizeof(timestamp));
  if (!in.good()) return false;
  
  // Read vector data
  if (!data_.Deserialize(in)) {
    return false;
  }
  
  // Since uid_ and timestamp_ are const, we can't modify them directly
  // The caller will need to handle this by constructing a new VectorRecord
  
  // For now, we'll use const_cast to get around this limitation
  // This is not ideal, but it's a quick solution for this specific case
  const_cast<uint64_t&>(uid_) = uid;
  const_cast<timestamp_t&>(timestamp_) = timestamp;
  
  return true;
}

} // namespace candy