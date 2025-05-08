#pragma once

namespace candy {

/**
 * @brief Enum defining the types of operators available in the system
 */
enum class OperatorType { 
  NONE,    // No specific operator type
  OUTPUT,  // Output operator
  FILTER,  // Filter operator
  MAP,     // Map/transform operator
  JOIN,    // Join operator
  SINK,    // Sink operator
  TOPK,    // Top-K operator 
  WINDOW   // Window operator
};

}  // namespace candy