# BriskFlow

BriskFlow is a vector-native stream processing engine for continuously evolving data. It provides a declarative Stream DAG, multicore execution, window-owned state, and indexed operators for similarity joins, TopK, filtering, aggregation, and snapshot materialization.

BriskFlow is the DataSys runtime behind the join-backed semantic windows demonstrated by **BriskSnapshot**. The authors report that BriskSnapshot has been accepted to the ICPP 2026 demo track; a public conference program or proceedings record is not yet available. BriskSnapshot combines this engine with orchestration and application-facing contracts; BriskFlow remains the framework-neutral data-system core.

> **Compatibility note**
>
> The C++ namespace, `SAGEFLOW_*` build/runtime switches, Python import surface, and `isage-flow` package name are retained temporarily for compatibility. New documentation and repository references use **BriskFlow**. Their eventual deprecation will be handled as a separately versioned API migration.

## Features

-   **Vector-Native Stream Processing**: At its core, BriskFlow is built to handle high-dimensional vector streams efficiently.
-   **Declarative API**: Easily compose complex, stateful vector operations such as `TopK`, `Filter`, and `Join` within defined temporal windows.
-   **Incremental Low-Latency Updates**: Optimized for incremental computations, ensuring semantic states are updated with minimal delay.
-   **Optimized Three-Phase Pipeline**: Abstracts stream processing into three distinct phases—ingestion, state materialization, and snapshot exposure—unlocking significant optimization opportunities.
-   **Stateful and Windowed Operations**: Natively supports windowing to create time-bound semantic snapshots from continuous data streams.

## Owners & Contact

-   **Owners**: @ZeroJustMe (ziao), @Pygone (xincai)
-   **Primary Contact**: @ZeroJustMe, @Pygone

For repository governance, release coordination, and external communication, use the two owners above as the official contacts.

## Key Use Cases

-   **Real-time LLM Generation**: Provide large language models with fresh, stateful context snapshots for more accurate and timely responses.
-   **Dynamic Context Maintenance**: Ideal for conversational AI or interactive applications where the context evolves rapidly over time.
-   **Streaming Data Analytics**: Serve high-velocity data analysis use cases that require complex, stateful semantic queries on vector data.
-   **Adaptive Recommendation Systems**: Build systems that can update recommendations in real-time based on the most recent user interactions and streaming events.

## Setup

To set up BriskFlow and its dependencies, first make sure that `docker` is installed, or use a **Linux** distribution with `apt`, such as Ubuntu or Debian.

We recommend starting with Docker while becoming familiar with BriskFlow.

### Quick Installation (Ubuntu/Debian)

For a quick one-click installation of all dependencies including DiskANN support, run:

```bash
cd <PATH_TO_REPO>
sudo ./scripts/install-deps.sh
```

This script will install:
- Build essentials (gcc, g++, cmake, etc.)
- DiskANN dependencies (libaio, boost, etc.)
- Intel MKL (Math Kernel Library)
- Environment configuration

After installation, reload your environment:
```bash
source /etc/profile.d/mkl.sh
```

### Docker

make sure you have installed `Docker` and `Docker` is running

#### Windows

```
cd <PATH_TO_REPO>/setup
./start_win.bat
```

#### Linux

```
cd <PATH_TO_REPO>/setup
./start.sh
```

### Linux with apt

check the dependencies in <PATH_TO_REPO>/setup/Dockerfile, and build your env

## BriskFlow examples

run the following commands to generate examples

```
cmake -B build
cmake --build build -j $(nproc)
```
This will generate the examples in the `build/bin` directory.
You can run the examples with:

```
./build/bin/itopk
```

## Ecosystem

- [DataSys](https://datasys.sage.org.ai/) owns and maintains the framework-neutral BriskFlow engine.
- [SAGE](https://sage.org.ai/) may consume BriskFlow through a thin integration adapter for application and workflow orchestration.
- [BriskSnapshot](https://datasys.sage.org.ai/achievements.html) is the join-backed semantic windows demonstration built on BriskFlow. Its authors report acceptance to the ICPP 2026 demo track; the public conference record is pending.

## Canonical repository

The canonical source, issue tracker, and contribution history are at [DataSysResearch/BriskFlow](https://github.com/DataSysResearch/BriskFlow).
