# CMakeLists.txt for execution module

add_lib(
        execution
        blocking_queue.cpp
        execution_graph.cpp
        execution_vertex.cpp
        input_gate.cpp
        result_partition.cpp
        ring_buffer_queue.cpp
)
target_link_libraries(
        execution
        common
        operator
)
