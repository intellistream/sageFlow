# CandyFlow System - Class Documentation

## 1. Overall Architecture

CandyFlow is a high-performance streaming approximate nearest neighbor search (ANNS) engine that combines stream processing with vector similarity search. The system follows a modular architecture with several key components that work together to provide efficient vector processing and searching capabilities.

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
| ConcurrencyMgr|
+---------------+
```

## 2. Core Components

### 2.1 Stream Processing System

#### StreamEnvironment
- **Description**: The entry point for stream processing, providing configuration and runtime context
- **Relationships**:
  - Contains StorageManager, ConcurrencyManager, Planner
  - Manages collections of Stream and Operator instances
  - Coordinates execution of the stream processing system
- **Key Methods**: `execute()`, `addStream()`, `getStorageManager()`, `getConcurrencyManager()`

#### StreamManager
- **Description**: Central coordinator for stream operations
- **Responsibilities**:
  - Manages time semantics, watermarks, and window progression
  - Coordinates operator execution and stream flow
  - Handles performance monitoring and metrics collection
- **Relationships**:
  - Contains references to Streams and Operators
  - Works with StorageManager for data persistence
- **Key Methods**: `buildOperatorGraph()`, `setAllowedLateness()`, `getMetrics()`

#### Stream
- **Description**: Represents a data stream with transformation capabilities
- **Responsibilities**:
  - Defines the flow of data from sources through operations to sinks
  - Provides fluent API for building processing pipelines
- **Relationships**:
  - Contains child Streams (tree structure)
  - Associated with Functions for operations
  - Processed by Operators
- **Key Methods**: `map()`, `filter()`, `join()`, `window()`, `sink()`

### 2.2 Operators and Functions

#### Operator (Abstract Base)
- **Description**: Base class for all operators that process stream elements
- **Responsibilities**:
  - Define the interface for stream processing operations
  - Process stream elements and emit results to downstream operators
- **Relationships**:
  - Forms a directed graph with other operators
  - Contains Function implementations
  - Processes StreamElements
- **Key Methods**: `process()`, `processDataElement()`, `emit()`, `addChild()`

#### Concrete Operators
- **FilterOperator**: Filters records based on a predicate function
- **MapOperator**: Transforms records using a mapping function
- **JoinOperator**: Joins records from multiple streams
- **WindowOperator**: Groups records into time windows
- **TopkOperator**: Finds top-k similar vectors
- **SinkOperator**: Outputs results to external systems

#### Function (Abstract Base)
- **Description**: Encapsulates the processing logic for operators
- **Responsibilities**:
  - Define the interface for stream processing functions
  - Implement specific logic for filtering, mapping, etc.
- **Relationships**:
  - Used by Operators to perform processing
- **Key Methods**: `getType()`, function-specific methods like `filter()`, `map()`, etc.

#### Concrete Functions
- **FilterFunction**: Contains filter predicate logic
- **MapFunction**: Contains data transformation logic
- **JoinFunction**: Contains record joining logic
- **WindowFunction**: Contains windowing logic
- **TopkFunction**: Contains similarity search logic
- **SinkFunction**: Contains output logic

### 2.3 Vector Storage and Indexing

#### StorageManager
- **Description**: Manages vector storage and retrieval operations
- **Responsibilities**:
  - Store and retrieve vector records
  - Manage memory for vector data
- **Relationships**:
  - Used by Indexes for vector access
  - Used by Operators for data persistence
  - Used by ConcurrencyManager for coordinated access
- **Key Methods**: `insert()`, `getVectorByUid()`, `remove()`

#### Index (Abstract Base)
- **Description**: Base class for vector indexing implementations
- **Responsibilities**:
  - Define interface for vector similarity search
  - Support insertion, deletion, and querying of vectors
- **Relationships**:
  - Uses StorageManager for vector storage
  - Used by ConcurrencyManager for vector operations
  - Used by TopkOperator for similarity search
- **Key Methods**: `insert()`, `erase()`, `query()`

#### Concrete Indexes
- **HNSW**: Implements Hierarchical Navigable Small World graph for approximate search
- **BruteForce**: Implements exact search by comparing with all vectors
- **IVF**: Implements Inverted File index for approximate search
- **VectraFlow**: Specialized index for streaming vector data

### 2.4 Compute Engine

#### ComputeEngine
- **Description**: Provides vector computation utilities
- **Responsibilities**:
  - Calculate similarity between vectors
  - Normalize vectors and perform mathematical operations
- **Relationships**:
  - Used by Indexes for similarity calculations
  - Used by Operators for vector processing
- **Key Methods**: `calcSimilarity()`, `calcEuclideanDistance()`, `normalizeVector()`, `getVectorSquareLength()`, `DotMultiply()`

### 2.5 Concurrency Management

#### ConcurrencyManager
- **Description**: Coordinates parallel operations on vectors and indexes
- **Responsibilities**:
  - Create and manage indexes
  - Coordinate concurrent access to vectors and indexes
- **Relationships**:
  - Uses StorageManager for vector operations
  - Contains ConcurrencyControllers for specific indexes
- **Key Methods**: `create_index()`, `drop_index()`, `insert()`, `erase()`, `query()`

#### ConcurrencyController
- **Description**: Controls access to specific indexes or storage
- **Responsibilities**:
  - Implement concurrency control for specific resources
  - Manage locks and synchronization
- **Relationships**:
  - Used by ConcurrencyManager
  - Controls access to Index instances
- **Key Methods**: Thread-safe versions of index operations

### 2.6 Query Optimization

#### Planner
- **Description**: Optimizes and plans query execution
- **Responsibilities**:
  - Transform stream definitions into operator graphs
  - Optimize query execution plans
- **Relationships**:
  - Used by StreamEnvironment
  - Creates Operator instances
- **Key Methods**: `plan()`

### 2.7 Data Types

#### StreamElement (Abstract Base)
- **Description**: Base class for all elements in a stream
- **Types**:
  - **DataElement**: Contains vector records
  - **Watermark**: Indicates event time progress
  - **EndOfStream**: Signals stream termination

#### VectorRecord
- **Description**: Represents a vector with its metadata
- **Responsibilities**:
  - Store vector data with its identifier and timestamp
  - Support serialization and comparison operations
- **Relationships**:
  - Contains VectorData
  - Processed by Operators and Functions
  - Stored by StorageManager
  - Indexed by Index implementations
- **Key Methods**: `getUid()`, `getTimestamp()`, `getData()`

#### VectorData
- **Description**: Represents raw vector data
- **Responsibilities**:
  - Store vector dimension, type, and component values
  - Support serialization and basic operations
- **Relationships**:
  - Used by VectorRecord
  - Processed by ComputeEngine
- **Key Methods**: `Serialize()`, `Deserialize()`

### 2.8 Time and Window Management

#### Window (Abstract Base)
- **Description**: Base class for time windowing strategies
- **Types**:
  - **SlidingWindow**: Windows with sliding behavior
  - **TumblingWindow**: Non-overlapping fixed-size windows

#### WatermarkGenerator
- **Description**: Generates watermarks for event time processing
- **Relationships**:
  - Used by StreamManager
  - Affects window trigger timing

## 3. Class Inheritance Hierarchies

### 3.1 Stream Processing Hierarchy
```
Stream
  ├── DataStreamSource
```

### 3.2 Operator Hierarchy
```
Operator (Abstract)
  ├── FilterOperator
  ├── MapOperator
  ├── JoinOperator
  ├── WindowOperator
  ├── TopkOperator
  └── SinkOperator
```

### 3.3 Function Hierarchy
```
Function (Abstract)
  ├── FilterFunction
  ├── MapFunction
  ├── JoinFunction
  ├── WindowFunction
  ├── TopkFunction
  └── SinkFunction
```

### 3.4 Index Hierarchy
```
Index (Abstract)
  ├── HNSW
  ├── BruteForce
  ├── IVF
  └── VectraFlow
```

### 3.5 Element Hierarchy
```
StreamElement (Abstract)
  ├── DataElement
  ├── Watermark
  └── EndOfStream
```

### 3.6 Window Hierarchy
```
Window (Abstract)
  ├── SlidingWindow
  └── TumblingWindow
```

## 4. Patterns and Design Principles

### 4.1 Key Design Patterns

- **Strategy Pattern**: Used in Function and Operator relationships
- **Builder Pattern**: Used in Stream's fluent interface
- **Observer Pattern**: Used for event notifications in stream processing
- **Factory Pattern**: Used in creating Index and Operator instances
- **Composite Pattern**: Used in the Stream tree structure

### 4.2 Memory Management Strategies

- **Ownership Semantics**:
  - `unique_ptr` used for exclusive ownership
  - `shared_ptr` used for shared ownership
  - References and raw pointers for non-owning references

### 4.3 Concurrency Model

- **Thread Safety**: ConcurrencyManager and Controllers ensure thread-safe operations
- **Lock Hierarchies**: Prevent deadlocks in concurrent operations
- **Parallelization**: Multiple threads for different operations

## 5. Best Practices and Areas for Improvement

### 5.1 Current Best Practices
- Dependency injection for better testability
- Clear separation of concerns with specialized components
- Interface-based design allowing multiple implementations

### 5.2 Areas for Improvement
- More consistent error handling
- Reduced coupling between components
- More comprehensive documentation
- Standardized memory management patterns
- Enhanced testing coverage

## 6. Example Usage Flow

1. Create a StreamEnvironment
2. Define data sources
3. Build a processing pipeline with Stream transformations
4. Execute the environment to process data
5. Retrieve results from sinks or query indexes

This architecture provides a flexible and extensible framework for high-performance streaming vector processing with approximate nearest neighbor search capabilities.