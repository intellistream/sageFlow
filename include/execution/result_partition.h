//
// Created by ZeroJustMe on 25-7-22.
//

#pragma once

#include "execution/ring_buffer_queue.h"
#include "execution/partitioner.h"
#include "common/data_types.h"
#include <vector>
#include <memory>

namespace candy {
class ResultPartition {
private:
  std::unique_ptr<IPartitioner> partitioner_;
  std::vector<QueuePtr> output_channels_;

public:
  void setup(std::unique_ptr<IPartitioner> p, const std::vector<QueuePtr>& channels);

  void emit(Response data) const;
};

}