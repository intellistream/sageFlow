//
// Created by ZeroJustMe on 25-7-22.
//

#include "execution/result_partition.h"

namespace candy {
void ResultPartition::setup(std::unique_ptr<IPartitioner> p, std::vector<QueuePtr> channels, int slot) {
  partitioner_ = std::move(p);
  channel_slot_map_[slot] = std::move(channels);
}

void ResultPartition::emit(Response&& data, int slot) const {
  if (!channel_slot_map_.contains(slot)) {
    throw std::runtime_error("ResultPartition::emit: Slot not found in channel map.");
  }
  const auto& output_channels_ = channel_slot_map_.at(slot);
  if (output_channels_.empty()) return;
  size_t channel_index = partitioner_->partition(data, output_channels_.size());
  output_channels_[channel_index]->push({std::move(data), slot});
}
}