## ADDED Requirements

### Requirement: Pair-reference materialization

When pair-passthrough materialization is selected, a Join match SHALL be materialized as a pair reference carrying the left record, the right record, and the similarity score, using read-only shared references to the original records. The legacy concat materialization MAY remain the default for backward compatibility until downstream pair-aware pipelines are migrated.

#### Scenario: Similar pair emitted as references
- **WHEN** the Join operator finds that a probe record and a candidate record are within the similarity threshold
- **THEN** it emits a result carrying a reference to the left original record, a reference to the right original record, and the similarity score, without producing a concatenated vector

#### Scenario: Original records preserved
- **WHEN** a downstream operator receives a materialized pair
- **THEN** it can read the unmodified original payload of both the left and right records

### Requirement: Zero vector copy on emit

The emit hot path SHALL NOT deep-copy `VectorData` bytes when materializing a pair result. Materializing one matched pair MUST add only reference counts plus at most one small payload allocation, not a copy of either vector body.

#### Scenario: No vector body copy per matched pair
- **WHEN** the emitter materializes one matched pair under the default mode
- **THEN** the number of `VectorData` allocations attributable to that pair is zero

#### Scenario: Probe and candidate shared, not copied
- **WHEN** the executor passes the probe record and a candidate to the emitter
- **THEN** both are passed as shared references and reused, not deep-copied into owning records

### Requirement: Additive pair transport

The transport layer SHALL carry pair results through an additive channel on the existing `Response` type with a dedicated response type value, so that existing `Record` and `List` transport paths and all existing operators continue to work without modification.

#### Scenario: Existing record path unchanged
- **WHEN** an upstream operator sends a normal `Response{Record}`
- **THEN** all existing downstream operators process it exactly as before, with no behavior change

#### Scenario: Pair response ignored by unaware operators
- **WHEN** an operator that does not understand pair results receives a pair response
- **THEN** it does not misinterpret it as a single record or a record list, and the default handling leaves correctness intact

### Requirement: Transport move semantics

The `Response` type SHALL support move construction and move assignment so that enqueuing a `Response` into the inter-operator queue transfers ownership without deep-copying record or pair payloads. Copy semantics MUST remain available for the broadcast path.

#### Scenario: Enqueue does not deep-copy
- **WHEN** a `Response` is moved into a `TaggedResponse` and pushed onto the ring buffer queue
- **THEN** no `VectorData` bytes are deep-copied as part of the enqueue

#### Scenario: Broadcast still copies
- **WHEN** a result is broadcast to multiple slots (slot == -1)
- **THEN** each slot receives an independent copy with correct payload, including the pair channel

### Requirement: LLM pre-processing join function contract

The system SHALL provide a selectable pair-passthrough materialization mode that packages the matched pair and the precomputed similarity without performing vector arithmetic, suitable for feeding two original records to a downstream LLM pre-processing task. The legacy concatenation join function SHALL remain available for pipelines that require a single output record.

#### Scenario: Pair-passthrough selected
- **WHEN** the user configures a Join for similarity identification feeding an LLM pre-processing task and selects pair-passthrough materialization
- **THEN** the Join emits the pair reference plus similarity, and does not allocate a concatenated vector

#### Scenario: Concatenation available on demand
- **WHEN** a pipeline explicitly requires a single concatenated vector output
- **THEN** the concatenation join function is still selectable and produces the concatenated record

### Requirement: Pair routing contract

Partitioners SHALL define a representative vector for a pair result so that content-based routing remains well-defined; by default the left record is the representative. Pipelines MAY declare round-robin partitioning to bypass content routing once pairs are formed.

#### Scenario: Default representative vector
- **WHEN** a content-based partitioner routes a pair result
- **THEN** it uses the left record's vector and timestamp as the routing key

#### Scenario: Round-robin bypass
- **WHEN** a downstream stage declares round-robin partitioning for pair results
- **THEN** pairs are distributed without computing a content-based key, and correctness does not depend on pair contents

### Requirement: Pair payload ownership and cross-thread free

The pair payload SHALL carry the two records as shared references by default and SHALL allocate them with a single combined allocation (e.g., `make_shared`). The design SHALL keep cross-thread deallocation cost measurable and provide a fallback allocation strategy without reverting to deep copies.

#### Scenario: Single combined allocation per record
- **WHEN** a record view backing a pair is created on the producing thread
- **THEN** its control block and object are allocated together so that the eventual free is a single deallocation, not two

#### Scenario: Cross-thread free is measured, not assumed
- **WHEN** the pair flows to a downstream operator on another thread and is destroyed there
- **THEN** a benchmark reports allocation count, p99, and allocator populate/scavenge share so the cross-thread free cost is observed rather than guessed

#### Scenario: Fallback without deep copy
- **WHEN** cross-thread free is shown to be a bottleneck
- **THEN** an arena/pool allocator or a uid-only pair payload is selectable, and neither fallback reintroduces a vector deep copy on the emit path
