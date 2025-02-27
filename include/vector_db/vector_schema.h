


#include <string>
#include <vector>
#include <map>

namespace candy {

struct VectorSchema {
  std::string name;
  std::map<std::string, std::string> metadata; // Metadata for schema
  size_t dimension;                           // Vector dimension
};

} // namespace candy


