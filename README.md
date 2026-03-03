## sageFlow

`sageFlow` is a cutting-edge, vector-native stream processing engine designed specifically to maintain and materialize semantic state snapshots for real-time, LLM-based generation tasks. The engine offers a declarative API to compose stateful vector operations within temporal windows, enabling fast and efficient updates to semantic context for dynamically changing datasets.

## Features

-   **Vector-Native Stream Processing**: At its core, sageFlow is built to handle high-dimensional vector streams efficiently.
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

To setup `sageFlow` and it's dependencies, begin by making sure that you have `docker` installed, or any **Linux** release version that contains `apt`, such as `Ubuntu` or `Debian`

We suggest first begin with `docker` before you are familiar with `sageFlow`.

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

## sageFlow: examples

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
