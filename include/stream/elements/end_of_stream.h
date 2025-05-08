#pragma once

#include "common/base_types.h"
#include "stream/elements/stream_element.h"

namespace candy {

/**
 * @brief Represents the end of a stream
 * 
 * EndOfStream signals that no more elements will arrive on a particular
 * stream, allowing the system to perform cleanup and finalization tasks.
 */
class EndOfStream : public StreamElement {
public:
    EndOfStream() : StreamElement(StreamElementType::END_OF_STREAM) {}
    // EndOfStream typically has no additional data
};

}  // namespace candy