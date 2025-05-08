#pragma once

namespace candy {

/**
 * @brief Enum defining the types of functions available in the system
 */
enum class FunctionType {  // NOLINT
  None,    // No specific function type
  Filter,  // Filter function type
  Map,     // Map/transform function type
  Join,    // Join function type
  Sink,    // Sink function type
  Topk,    // Top-K function type
  Window   // Window function type (all caps for consistency with usage)
};

}  // namespace candy