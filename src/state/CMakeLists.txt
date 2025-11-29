# CMakeLists.txt for state module

add_lib(
        state
        partitioned_window_state.cpp
        partitioned_vector_state.cpp
        shared_window_state.cpp
        two_tier_window_state.cpp
)
target_link_libraries(
        state
        PRIVATE
        common
        execution
        coordination
)
