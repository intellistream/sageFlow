//
// Created by Pygon on 25-3-14.
//
#include "stream/data_stream_source/data_stream_source.h"

sageFlow::DataStreamSource::DataStreamSource(std::string name, const DataStreamSourceType type)
    : Stream(std::move(name)), type_(type) {}

auto sageFlow::DataStreamSource::getType() const -> DataStreamSourceType { return type_; }

void sageFlow::DataStreamSource::setType(const DataStreamSourceType type) { type_ = type; }
