#pragma once
#include <assert.h>

#include <functional>
#include <memory>

#include "common/data_types.h"  // Include for VectorRecord
#include "function/function.h"
#include "stream/stream.h"

namespace candy {
// Forward declarations
class Window;
class Stream;

using JoinFunc =
    std::function<std::unique_ptr<VectorRecord>(std::unique_ptr<VectorRecord> &, std::unique_ptr<VectorRecord> &)>;

class JoinFunction final : public Function {
 public:
  explicit JoinFunction(std::string name);

  explicit JoinFunction(std::string name, JoinFunc join_func);
  explicit JoinFunction(std::string name, JoinFunc join_func, int64_t time_window);

  // Update the Execute method to use DataElement instead of Response
  auto Execute(DataElement &left, DataElement &right) -> DataElement override;

  auto setJoinFunc(JoinFunc join_func) -> void;

  auto getOtherStream() -> std::shared_ptr<Stream> &;

  auto setOtherStream(std::shared_ptr<Stream> other_plan) -> void;

  auto setWindow(int64_t window_size, int64_t slide_size) -> void;

  std::shared_ptr<Window> windowL;
  std::shared_ptr<Window> windowR;

 private:
  JoinFunc join_func_;
  std::shared_ptr<Stream> other_stream_ = nullptr;
  // Using the new window system for time-based joining
};
}  // namespace candy