//
// Created by Pygon on 25-3-14.
//
#include "streaming/data_stream_source/data_stream_source.h"

candy::DataStreamSource::DataStreamSource(std::string name, const DataStreamSourceType type)
    : Stream(std::move(name)), type_(type) {}

auto candy::DataStreamSource::getType() const -> DataStreamSourceType { return type_; }

void candy::DataStreamSource::setType(const DataStreamSourceType type) { type_ = type; }
