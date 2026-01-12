# CMakeLists.txt for execution module

add_lib(
        execution
        blocking_queue.cpp
        centroid_partitioner.cpp
        clustered_partitioner.cpp
        connection_strategy.cpp
        execution_graph.cpp
        execution_vertex.cpp
        input_gate.cpp
        partitioner_factory.cpp
        result_partition.cpp
        ring_buffer_queue.cpp
        vector_space_partitioner.cpp
)
target_link_libraries(
        execution
        PRIVATE
        common
        operator
)
