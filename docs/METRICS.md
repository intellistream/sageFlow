# Metrics and Monitoring System Documentation

## Overview

The sageFlow metrics system provides comprehensive performance monitoring infrastructure using both:

1. **GPERFTOOLS**: Detailed CPU and heap profiling for identifying bottlenecks
2. **Fine-grained Metrics**: Real-time aggregated statistics with zero overhead when disabled

## Architecture

### Core Infrastructure (`include/utils/monitoring.h`)

Unified monitoring header that provides all performance monitoring tools:

#### GPERFTOOLS Integration

**PerformanceMonitor** class provides access to gperftools profiling:

- **CPU Profiling**: Identify performance hotspots and function call frequencies
- **Heap Profiling**: Track memory allocations and find memory leaks (tcmalloc)
- **Analysis**: Use `pprof` to generate reports from profile data

Usage:
```cpp
#include "utils/monitoring.h"

PerformanceMonitor monitor("my_profile.prof");
monitor.StartProfiling();
// ... code to profile ...
monitor.StopProfiling();

// Analyze with: pprof --text ./your_binary my_profile.prof
// Or generate call graph: pprof --pdf ./your_binary my_profile.prof > profile.pdf
```

**What GPERFTOOLS can monitor:**
- CPU time spent in each function (sampling-based)
- Function call counts and call graph
- Memory allocation patterns and sizes
- Memory leaks and heap growth
- Lock contention (with heap profiler extensions)

**When to use GPERFTOOLS:**
- Development and testing: Identify performance bottlenecks
- One-time profiling runs to understand system behavior
- Memory leak detection during long-running tests
- Detailed call graph analysis

#### Fine-Grained Metrics Classes

**`ScopedTimerAtomic`**
- RAII timer that measures elapsed time and adds it to an atomic counter
- Use for measuring specific code sections
- Usage:
  ```cpp
  std::atomic<uint64_t> my_timing_metric{0};
  {
    ScopedTimerAtomic timer(my_timing_metric);
    // ... code to measure ...
  } // Time automatically recorded
  ```

**`ScopedAccumulateAtomic`**
- RAII accumulator for measuring time from a pre-captured timestamp
- Useful when you need to measure time including lock wait
- Usage:
  ```cpp
  uint64_t start = ScopedAccumulateAtomic::now_ns();
  // ... some work ...
  {
    ScopedAccumulateAtomic acc(my_metric, start);
    // ... more work ...
  } // Total time from start is recorded
  ```

**`MetricsTimer`**
- Conditionally compiled RAII timer
- When metrics disabled, compiles to empty class with no overhead
- Usage:
  ```cpp
  {
    MetricsTimer timer(my_metric);
    // ... code to measure ...
  }
  ```

#### Helper Functions

All helpers are conditionally compiled based on `SAGEFLOW_ENABLE_METRICS`:

- **`metrics_timestamp()`**: Get current timestamp (0 when metrics disabled)
- **`metrics_record_elapsed(metric, start_time)`**: Record elapsed time to one metric
- **`metrics_record_elapsed_dual(metric1, metric2, start_time)`**: Record to two metrics
- **`metrics_increment(counter, value)`**: Increment a counter metric

### Operator-Specific Metrics (`include/utils/metrics/`)

Operator-specific metrics are organized under `utils/metrics/`:

#### Join Operator Metrics (`include/utils/metrics/join_metrics.h`)

Join-specific metrics implementation built on top of the core infrastructure:

**JoinMetrics Structure**

Singleton container for all join operator metrics:

**Timing Metrics** (in nanoseconds):
- `window_insert_ns`: Time for window insert/expire operations
- `index_insert_ns`: Time for index operations
- `candidate_fetch_ns`: Time fetching join candidates
- `similarity_ns`: Time computing similarity
- `join_function_ns`: Time executing join function
- `emit_ns`: Time emitting results
- `lock_wait_ns`: Time waiting for locks
- `apply_processing_ns`: Total time in apply() method

**Counter Metrics**:
- `total_records_left/right`: Records processed per side
- `total_emits`: Results emitted
- `window_records_left/right_completed`: Records expired
- `apply_processing_count`: Number of apply() calls
- `e2e_latency_ns/count`: End-to-end latency tracking

**Join-Specific Helpers**:
- `metrics_record_lock_wait(start_time)`: Record to lock_wait_ns
- `metrics_record_lock_wait_dual(start_time, additional_metric)`: Record to lock_wait_ns and another metric
- `metrics_record_e2e_latency(start_time)`: Record end-to-end latency

## Adding Metrics to New Operators

### Step 1: Create Operator-Specific Metrics Header

Create `include/utils/metrics/your_operator_metrics.h`:

```cpp
#pragma once
#include <atomic>
#include <cstdint>
#include "utils/monitoring.h"

namespace sageFlow {

struct YourOperatorMetrics {
  // Define your metrics
  std::atomic<uint64_t> processing_ns{0};
  std::atomic<uint64_t> records_processed{0};
  
  static YourOperatorMetrics& instance() {
    static YourOperatorMetrics inst;
    return inst;
  }
  
  void reset() {
    processing_ns = 0;
    records_processed = 0;
  }
};

// Optional: Add operator-specific helper functions
inline void your_operator_specific_helper(uint64_t start_time) {
#ifdef SAGEFLOW_ENABLE_METRICS
  metrics_record_elapsed(YourOperatorMetrics::instance().processing_ns, start_time);
#else
  (void)start_time;
#endif
}

} // namespace sageFlow
```

### Step 2: Use Metrics in Your Operator

```cpp
#include "utils/metrics/your_operator_metrics.h"
#include "utils/monitoring.h"  // For GPERFTOOLS if needed

void YourOperator::process() {
  // Optional: Use GPERFTOOLS for detailed profiling (development/testing)
  #ifdef ENABLE_GPERFTOOLS
  PerformanceMonitor monitor("your_operator_profile.prof");
  monitor.StartProfiling();
  #endif
  
  // Use MetricsTimer for scoped timing (production monitoring)
  MetricsTimer timer(YourOperatorMetrics::instance().processing_ns);
  
  // Use metrics_increment for counters (always call, not conditional)
  YourOperatorMetrics::instance().records_processed.fetch_add(1, std::memory_order_relaxed);
  
  // Or use helper if you created one
  uint64_t start = metrics_timestamp();
  // ... work ...
  your_operator_specific_helper(start);
  
  #ifdef ENABLE_GPERFTOOLS
  monitor.StopProfiling();
  #endif
}
```

## Comparison: GPERFTOOLS vs Fine-Grained Metrics

| Feature | GPERFTOOLS | Fine-Grained Metrics |
|---------|-----------|---------------------|
| **Use Case** | Development, debugging, one-time analysis | Production, continuous monitoring |
| **Overhead** | ~1-5% CPU overhead | Zero when disabled, <0.1% when enabled |
| **Granularity** | Function-level (sampling) | Code-block level (exact) |
| **Output** | Profile files (.prof) | Real-time counters |
| **Analysis** | Post-processing with pprof | Real-time or export to TSV |
| **Call Graph** | Yes, detailed | No |
| **Memory Profiling** | Yes (heap, leaks) | No |
| **Always On** | No (development only) | Yes (production-safe) |

## Best Practices

### Conditional vs Unconditional Metrics

- **Conditional** (wrapped in `#ifdef`): Use `MetricsTimer`, `metrics_increment()`, helper functions
  - These compile to no-ops when metrics disabled
  - Use for timing and optional counters

- **Unconditional** (always executed): Direct atomic operations
  - Use when tests or other code depends on the counter
  - Example: `metric.fetch_add(1, std::memory_order_relaxed)`

### Combining GPERFTOOLS and Fine-Grained Metrics

**Development Workflow:**
1. Enable fine-grained metrics to identify problem areas
2. Use GPERFTOOLS profiling for detailed analysis of hot spots
3. Optimize based on both metrics
4. Verify with fine-grained metrics in production

**Example:**
```cpp
void critical_section() {
  // Fine-grained metric (always on in production)
  MetricsTimer timer(MyMetrics::instance().critical_section_ns);
  
  // GPERFTOOLS profiling (development only)
  #ifdef ENABLE_GPERFTOOLS
  // This section will show up in pprof call graph
  #endif
  
  // ... critical code ...
}
```

### Thread Safety

All metrics use `std::atomic` with `memory_order_relaxed` for:
- Lock-free operation
- Minimal performance impact
- Correct concurrent access

Note: Relaxed ordering is sufficient because metrics don't synchronize program logic.

## Example: Join Operator Integration

See `src/operator/join_operator.cpp` for a comprehensive example showing:
- GPERFTOOLS integration for detailed profiling
- Fine-grained metrics for production monitoring
- Lock wait tracking
- End-to-end latency measurement
- Counter metrics for pipeline health

## Analyzing GPERFTOOLS Output

After profiling with PerformanceMonitor:

```bash
# Text report showing top functions
pprof --text ./your_binary profile.prof

# Interactive web view
pprof --web ./your_binary profile.prof

# Generate PDF call graph
pprof --pdf ./your_binary profile.prof > callgraph.pdf

# Focus on specific function
pprof --focus=FunctionName --text ./your_binary profile.prof
```

## Join Operator GPERFTOOLS Integration

### Configuration

The Join Operator supports built-in GPERFTOOLS profiling that can be enabled via configuration:

**In `config/join_config.toml`:**
```toml
# GPERFTOOLS Profiling (requires ENABLE_GPERFTOOLS=ON during build)
enableProfiling = true
profileOutputPath = "profiles/join_operator_profile.prof"
```

**Parameters:**
- `enableProfiling`: Set to `true` to enable CPU profiling
- `profileOutputPath`: Path where profile data will be saved (default: `profiles/join_operator_profile.prof`)

### How It Works

When profiling is enabled:
1. **Initialization**: Profiler is created when JoinOperator is constructed
2. **Start**: Profiling starts when `open()` is called (operator becomes active)
3. **Stop**: Profiling stops when the operator is destroyed (pipeline completion)

This captures the complete execution profile of the join operator including:
- Window management operations
- Index operations (IVF/BruteForce)
- Similarity computations
- Join function execution
- Lock wait times

### Using the Profile Data

After running with profiling enabled:

```bash
# Generate text report of top functions
pprof --text ./build/bin/your_test profiles/join_operator_profile.prof

# Generate call graph (requires graphviz)
pprof --pdf ./build/bin/your_test profiles/join_operator_profile.prof > join_callgraph.pdf

# Interactive web view
pprof --web ./build/bin/your_test profiles/join_operator_profile.prof

# Focus on specific functions
pprof --focus=JoinOperator --text ./build/bin/your_test profiles/join_operator_profile.prof
```

### Build Requirements

GPERFTOOLS profiling requires the following:

1. **Build with GPERFTOOLS enabled:**
   ```bash
   cmake -DENABLE_GPERFTOOLS=ON ..
   make
   ```

2. **Install gperftools** (if not already installed):
   ```bash
   # Ubuntu/Debian
   sudo apt-get install google-perftools libgoogle-perftools-dev
   
   # macOS
   brew install gperftools
   ```

3. **Install pprof analysis tools:**
   ```bash
   # Ubuntu/Debian
   sudo apt-get install google-perftools
   
   # Or use Go version
   go install github.com/google/pprof@latest
   ```

### Example Workflow

1. **Enable profiling in config:**
   ```toml
   enableProfiling = true
   profileOutputPath = "profiles/join_perf.prof"
   ```

2. **Run your test/application:**
   ```bash
   ./build/bin/your_test
   ```

3. **Analyze the results:**
   ```bash
   # See top CPU consumers
   pprof --text ./build/bin/your_test profiles/join_perf.prof
   
   # Generate visual call graph
   pprof --pdf ./build/bin/your_test profiles/join_perf.prof > analysis.pdf
   ```

4. **Optimize based on findings** and repeat

### Best Practices

- **Development**: Enable profiling to identify bottlenecks during development
- **Testing**: Profile specific test scenarios to understand performance characteristics
- **Production**: Disable profiling (set `enableProfiling = false`) to avoid overhead
- **Output Path**: Use descriptive names like `profiles/join_ivf_eager_profile.prof` to distinguish different configurations
- **Combine with Metrics**: Use fine-grained metrics for continuous monitoring, GPERFTOOLS for deep analysis

