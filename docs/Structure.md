# CandyFlow Project Structure

## Overview

CandyFlow is a stream processing system for vector data with support for high-performance similarity search, windowed operations, and real-time processing. The system follows a modular architecture organized into several logical components.

## Core Components

### 1. Stream Processing System

- **Stream Manager**: Central coordinator for stream operations
  - Manages time semantics, watermarks, and window progression
  - Coordinates operator execution and stream flow
  - Handles performance monitoring and metrics collection
  
- **Stream**: Represents a data stream with transformation capabilities
  - Supports fluent API for operation chaining
  - Provides methods for filtering, mapping, windowing, joining, and more
  
- **Operators**: Building blocks that perform operations on data
  - Types: Filter, Map, Join, Sink, TopK, Window
  - Form a directed graph of operations that process data

- **Functions**: Encapsulate the actual logic for operations
  - Abstract interfaces for different operation types
  - User-defined implementations for customized processing

### 2. Vector Index and Storage

- **Storage Manager**: Handles vector record storage and retrieval
  - Manages vector records with associated metadata
  - Provides interfaces for inserting, retrieving, and removing vectors
  
- **Index**: Implements vector similarity search
  - Types: HNSW, BruteForce
  - Supports k-nearest neighbor search for vectors
  
- **Compute Engine**: Provides vector computation utilities
  - Implements similarity metrics (cosine, Euclidean)
  - Provides vector normalization and other operations

### 3. Concurrency Management

- **Concurrency Manager**: Coordinates parallel operations
  - Creates and manages indexes
  - Provides thread-safe access to storage and index operations
  
- **Concurrency Controller**: Manages access to shared resources
  - Controls concurrent operations on indexes and storage
  - Implements appropriate locking strategies

### 4. Common Utilities

- **Data Types**: Core data structures used throughout the system
  - VectorRecord: Primary data unit with vectors and metadata
  - Response: Wrapper for data returned by operators
  - Windows, Watermarks, Punctuations: Stream processing concepts

## Class Relationships Diagram

```
+---------------+      +---------------+      +---------------+
| StreamManager |----->| Stream        |----->| Operator      |
+---------------+      +---------------+      +---------------+
       |                      |                      |
       |                      |                      |
       v                      v                      v
+---------------+      +---------------+      +---------------+
| StorageManager|      | Function      |      | Function      |
+---------------+      +---------------+      +---------------+
       |                                              
       |                                              
       v                                              
+---------------+      +---------------+              
| Index         |----->| ComputeEngine |              
+---------------+      +---------------+              
       |
       |
       v
+---------------+
| Concurrency   |
| Manager       |
+---------------+
```

## Challenges in Current Architecture

1. **Tight Coupling**: Some components have strong dependencies on each other, making the system less flexible and harder to test.

2. **Inconsistent Memory Management**: Mix of raw pointers, unique_ptr, and shared_ptr without clear ownership semantics.

3. **Limited Abstraction**: Some implementations leak through interfaces rather than being properly encapsulated.

4. **Incomplete Documentation**: Many classes lack proper documentation on their purpose and usage.

## Optimization Suggestions

### 1. Improve Class Structure and Dependencies

- **Dependency Injection**: Use dependency injection throughout the codebase to reduce direct dependencies between components.
- **Interface-Based Design**: Define clearer interfaces for major components and implement concrete classes behind these interfaces.
- **Component Isolation**: Better isolate components to make them independently testable and replaceable.

### 2. Memory Management Standardization

- **Ownership Policy**: Define clearer ownership semantics for objects throughout the system.
- **Smart Pointer Strategy**: Standardize smart pointer usage:
  - Use `std::unique_ptr` for exclusive ownership
  - Use `std::shared_ptr` for shared ownership
  - Avoid raw pointers for ownership
  - Use references or pointers for non-owning references

### 3. API Improvements

- **Consistent Error Handling**: Implement consistent error handling strategy (exceptions vs. error codes).
- **Fluent Interface**: Extend the fluent interface throughout the API for more readable code.
- **Parameter Validation**: Add better validation of input parameters.

### 4. Performance Optimizations

- **Memory Pool**: Implement memory pooling for frequently allocated vector data.
- **Thread Pool**: Create a central thread pool for better resource utilization.
- **Lock-Free Algorithms**: Replace mutex-based synchronization with lock-free algorithms where appropriate.

### 5. Testing and Documentation

- **Unit Testing**: Increase unit test coverage for all components.
- **Integration Testing**: Add integration tests for component interactions.
- **API Documentation**: Improve code documentation with clear descriptions of class responsibilities, method contracts, and usage examples.

## Implementation Prioritization

1. Define clearer interfaces and reduce coupling between components
2. Standardize memory management and ownership semantics
3. Implement consistent error handling
4. Add comprehensive unit and integration tests
5. Improve documentation
6. Optimize performance-critical paths