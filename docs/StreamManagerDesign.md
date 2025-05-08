# Stream Manager Design (`StreamManager`)

`StreamManager` is the stream processing coordination layer in `candyFlow`. It is responsible for managing the overall stream graph execution, coordinating time semantics (event time, watermarks, windowing), handling stream alignment (especially for multi-input operators like joins), processing out-of-order data, and potentially facilitating stream feature extraction ($\phi(S)$).

## 1. Time Semantics and Window Progression

**Requirement:** Define and manage time semantics (event time vs. processing time) and progress time using watermarks.

**Implementation Details:**

*   **Time Model:**
    *   Primarily support **Event Time**. Assume each `Record` (defined in `common/data_types.h`) has a `timestamp_` field representing its event time.
    *   Processing Time can be supported implicitly if operators use system time, but event time should be the default and explicitly managed.
    *   Mixed time semantics are complex; focus on robust event time first.
*   **Watermark Generation:**
    *   Watermarks ($\tau(t)$) indicate that no records with `timestamp <= t` are expected.
    *   Implement watermark generation primarily at the source (`DataStreamSource`). Sources should periodically emit `Punctuation` objects (a special record type or signal) containing the watermark timestamp.
    *   The strategy can be based on the minimum observed event time at the source, potentially with heuristics for idleness.
*   **Watermark Propagation:**
    *   `StreamManager` or the `StreamEnvironment` should ensure watermarks flow through the operator graph.
    *   Operators (`operator/*`) must forward watermarks downstream. For operators that buffer data (like windows or joins), they should only emit a new watermark `w` when their internal minimum pending event time is `>= w`.
    *   For multi-input operators, the operator's current watermark is the *minimum* of the watermarks received from all inputs. This ensures alignment.
*   **Checkpointing:**
    *   Currently, no checkpointing mechanism is integrated with watermarks. Checkpointing would require saving operator state aligned with specific watermarks (similar to Flink's barriers). This is a future consideration.

## 2. Out-of-Order Data and Lateness

**Requirement:** Handle records arriving after the watermark has passed their timestamp.

**Implementation Details:**

*   **Buffering:** Operators that rely on time order (e.g., `WindowOperator`, `JoinOperator`) must buffer records based on their event time (`Record::timestamp_`). Watermarks trigger processing for records up to the watermark timestamp.
*   **Allowed Lateness:**
    *   Implement an "allowed lateness" mechanism for windowing. `WindowOperator` should accept records with `timestamp > window.end` but `timestamp <= watermark + allowed_lateness`.
    *   The window state should be kept until `watermark > window.end + allowed_lateness`.
*   **Handling Late Data (Beyond Lateness):**
    *   **Default:** Drop late data arriving after the allowed lateness period.
    *   **(Optional) Side Output:** Modify `WindowOperator` to emit truly late data to a separate "side output" stream for alternative processing or logging. This requires extending the `Stream` and operator API.

## 3. Window Management Logic

**Requirement:** Define and manage windowing semantics (Tumbling, Sliding, Session).

**Implementation Details:**

*   **Window Operators:** Implement windowing logic within dedicated operators (e.g., `TumblingWindowOperator`, `SlidingWindowOperator`) inheriting from a base `Operator` class.
*   **Window Definition:** Users define windows (type, size, slide) when building the stream graph using the `Stream` API (e.g., `stream.window(TumblingEventTimeWindows.of(size))`).
*   **Window Instantiation:** The `WindowOperator` receives records and assigns them to one or more `Window` instances based on their timestamp and the window definition.
*   **State Management:**
    *   Each instance of a `WindowOperator` maintains the state for its active windows (e.g., buffered records, intermediate aggregation results).
    *   This state is typically held in memory (e.g., `std::map<Window, State>`). For larger state, integration with the `storage` module could be considered, but start with in-memory state.
*   **Window Evaluation:** Watermarks trigger the evaluation of windows. When `watermark >= window.end`, the `WindowOperator` finalizes the computation for that window and emits the result(s).
*   **State Clearing:** Window state is cleared when `watermark > window.end + allowed_lateness`. Implement a cleanup mechanism within the `WindowOperator` triggered by watermark progression.

## 4. Stream Feature Extraction $\phi(S)$ (Monitoring)

**Requirement:** Extract runtime features or metrics from the stream.

**Implementation Details:**

*   **Metrics Collection:**
    *   Implement a dedicated `MonitorOperator` or add monitoring capabilities to existing operators or the `StreamEnvironment`.
    *   Collect metrics like record counts, throughput (records/sec), latency (processing time - event time, requires watermark context), watermark lag.
*   **Granularity:** Collect metrics per-operator instance, potentially aggregated globally or per-stream type.
*   **Reporting:**
    *   Store metrics in a shared data structure, possibly managed by `StreamEnvironment` or a dedicated monitoring singleton.
    *   Use thread-safe structures (e.g., atomic counters, mutex-protected maps) for updates.
    *   Provide an API (e.g., methods on `StreamEnvironment`) to query these metrics.
    *   **(Optional) Persistence:** Log metrics periodically using `spdlog` or expose them via a simple endpoint if needed.

## 5. Multi-Input Alignment and Join Support

**Requirement:** Support joining multiple streams based on time windows.

**Implementation Details:**

*   **Join Operator:** Enhance the existing `JoinOperator`.
*   **Time-Based Buffering:** The `JoinOperator` must buffer records from *both* input streams, keyed by their event time (`Record::timestamp_`) or assigned window.
*   **Watermark Alignment:** The `JoinOperator` tracks the minimum watermark across all its inputs. This combined watermark dictates the progress of event time within the operator.
*   **Join Triggering:** When the combined watermark advances, the `JoinOperator` processes records in its buffers up to the watermark. For windowed joins, it evaluates and emits results for windows where `window.end <= watermark`.
*   **Join Window:** The join logic operates on windows defined implicitly by time or explicitly using a window assigner (similar to single-stream windowing). State for each join window (buffered records from both sides) is managed within the `JoinOperator`.
*   **Backpressure:** Currently not explicitly handled based on stream speeds. Requires integration with the execution engine (`compute_engine`?) or `concurrency` layer to slow down faster inputs if buffers in the `JoinOperator` become full. This is a complex feature requiring careful design.

## 6. Visualization and Debugging Support

**Requirement:** Provide means to observe and debug the stream processing flow.

**Implementation Details:**

*   **Logging:** Add detailed logging (`spdlog`) within `StreamManager`, `StreamEnvironment`, `DataStreamSource`, and key operators (`WindowOperator`, `JoinOperator`). Log watermark generation/propagation, window assignments/evaluations/cleanups, and handling of late data.
*   **Metrics Exposure:** Expose the metrics collected in Section 4. This provides high-level visibility into stream health (rates, latency, watermarks).
*   **(Future) Visualization:** A dedicated UI or dashboard could query the metrics API to visualize the stream graph, throughput, latency, and watermark status.
