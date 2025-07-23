//
// Created by ZeroJustMe on 25-7-22.
//

#include "execution/result_partition.h"

namespace candy {
void ResultPartition::setup(std::unique_ptr<IPartitioner> p, const std::vector<QueuePtr>& channels) {
  partitioner_ = std::move(p);
  output_channels_ = channels;
}

void ResultPartition::emit(Response data) const {
  if (output_channels_.empty()) return;
  size_t channel_index = partitioner_->partition(data, output_channels_.size());
  output_channels_[channel_index]->push(std::move(data));
}
}